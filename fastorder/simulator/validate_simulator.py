from fastorder.db.connection import get_engine
from sqlalchemy import text

def validate_simulator():
    errors_found = 0
    engine = get_engine()
    print("Bắt đầu tiến trình validation")
    with engine.connect() as conn:

        query_check_data = text("""SELECT COUNT(*)
        FROM orders
        WHERE source_system = 'simulator';""")
        simulated_order_count  = conn.execute(query_check_data).scalar_one()
        if simulated_order_count == 0:
            raise ValueError("Prerequisite Failed: Không có simulated order để validation.")
        print(f"Prerequisite PASSED: Có {simulated_order_count} simulated orders để validation.")

        # RULE 1: Mọi đơn hàng simulator phải có ít nhất 1 order_item
        rule1_query = text("""
            SELECT o.order_id
            FROM orders o
            LEFT JOIN order_items oi ON o.order_id = oi.order_id
            WHERE o.source_system = 'simulator'
            GROUP BY o.order_id
            HAVING COUNT(oi.order_item_id) = 0;
        """)
        rule1_fails = conn.execute(rule1_query).fetchall()
        if rule1_fails:
            print(f"RULE 1 FAILED: Có {len(rule1_fails)} đơn hàng không có sản phẩm nào!")
            errors_found += 1
        else:
            print("RULE 1 PASSED: 100% đơn hàng đều có sản phẩm.")

        # RULE 2: Mọi đơn hàng simulator phải có đúng 1 payment
        rule2_query = text("""
            SELECT o.order_id, COUNT(op.payment_sequential) as pay_count
            FROM orders o
            LEFT JOIN order_payments op ON o.order_id = op.order_id
            WHERE o.source_system = 'simulator'
            GROUP BY o.order_id
            HAVING COUNT(op.payment_sequential) != 1;
        """)
        rule2_fails = conn.execute(rule2_query).fetchall()
        if rule2_fails:
            print(f"RULE 2 FAILED: Có {len(rule2_fails)} đơn hàng bị thiếu hoặc thừa payment!")
            errors_found += 1
        else:
            print("RULE 2 PASSED: 100% đơn hàng có đúng 1 payment.")

        # RULE 3: payment_value = SUM(price * quantity + freight_value)
        # Sử dụng ABS(a - b) > 0.01 Cho phép sai lệch tối đa 0.01 do quy tắc làm tròn tiền tệ.
        rule3_query = text("""
            SELECT o.order_id,
                op.payment_value,
                COALESCE(SUM(oi.price * oi.quantity + oi.freight_value), 0) AS calculated_value
            FROM orders o
            JOIN order_payments op ON o.order_id = op.order_id
            JOIN order_items oi ON o.order_id = oi.order_id
            WHERE o.source_system = 'simulator'
            GROUP BY o.order_id, op.payment_value
            HAVING ABS(op.payment_value - COALESCE(SUM(oi.price * oi.quantity + oi.freight_value), 0)) > 0.01;
        """)
        rule3_fails = conn.execute(rule3_query).fetchall()
        if rule3_fails:
            print(f"RULE 3 FAILED: Có {len(rule3_fails)} đơn hàng bị lệch tiền (Reconciliation Mismatch)!")
            sample = rule3_fails[0]
            print(f"     -> Ví dụ Order {sample[0][:8]}: Database ghi ${sample[1]}, nhưng tính toán thực tế ra ${sample[2]}")
            errors_found += 1
        else:
            print("RULE 3 PASSED: 100% hóa đơn khớp dòng tiền từng đồng.")


        # RULE 4: orders.warehouse_id phải trùng với warehouse_id của order_items
        # Đảm bảo không có chuyện đơn hàng gán kho A nhưng lại lấy đồ từ kho B.
        # IS DISTINCT FROM xử lý được cả trường hợp NULL.
        rule4_query = text("""
            SELECT o.order_id, o.warehouse_id AS order_wh, oi.warehouse_id AS item_wh
            FROM orders o
            JOIN order_items oi ON o.order_id = oi.order_id
            WHERE o.source_system = 'simulator'
            AND (
                o.warehouse_id IS NULL
                OR oi.warehouse_id IS NULL
                OR o.warehouse_id IS DISTINCT FROM oi.warehouse_id
            );
        """)
        rule4_fails = conn.execute(rule4_query).fetchall()
        if rule4_fails:
            print(f"RULE 4 FAILED: Có {len(rule4_fails)} đơn hàng bị lệch kho (Cross-warehouse Mismatch)!")
            errors_found += 1
        else:
            print("RULE 4 PASSED: 100% item đều được xuất từ đúng kho của đơn hàng.")

        # RULE 5: quantity trong order_items phải > 0
        rule5_query = text("""
            SELECT order_id, order_item_id, quantity
            FROM order_items
            WHERE order_id IN (SELECT order_id FROM orders WHERE source_system = 'simulator')
            AND (quantity <= 0 OR quantity IS NULL);
        """)
        rule5_fails = conn.execute(rule5_query).fetchall()
        if rule5_fails:
            print(f"RULE 5 FAILED: Phát hiện {len(rule5_fails)} order_item có số lượng mua <= 0!")
            errors_found += 1
        else:
            print("RULE 5 PASSED: Không có mặt hàng nào mua với số lượng <= 0.")

        # RULE 6: inventory.quantity_available không được âm
        # Đây là test cực kỳ quan trọng để đảm bảo không bị Overselling
        rule6_query = text("""
            SELECT warehouse_id, product_id, quantity_available
            FROM inventory
            WHERE quantity_available < 0;
        """)
        rule6_fails = conn.execute(rule6_query).fetchall()
        if rule6_fails:
            print(f"RULE 6 FAILED: CẢNH BÁO OVERSELLING! Có {len(rule6_fails)} dòng tồn kho bị âm.")
            sample = rule6_fails[0]
            print(f"     -> Ví dụ: Kho {sample[0]}, SP {sample[1]} đang có tồn kho = {sample[2]}")
            errors_found += 1
        else:
            print("RULE 6 PASSED: Tồn kho an toàn, không có số âm.")

        # RULE 7: Kiểm soát source_system
        # Đảm bảo dữ liệu không bị "rác" bởi các nguồn hệ thống lạ
        # ==========================================
        rule7_query = text("""
            SELECT order_id, source_system
            FROM orders
            WHERE source_system IS NULL OR source_system NOT IN ('olist_seed', 'simulator');
        """)
        rule7_fails = conn.execute(rule7_query).fetchall()
        if rule7_fails:
            print(f"RULE 7 FAILED: Phát hiện {len(rule7_fails)} đơn hàng có source_system lạ!")
            errors_found += 1
        else:
            print("RULE 7 PASSED: 100% đơn hàng thuộc đúng nguồn (olist_seed hoặc simulator).")

        # RULE 8: Status hợp lệ
        rule8_query = text("""
            SELECT order_id, order_status
            FROM orders
            WHERE source_system = 'simulator'
              AND (order_status IS NULL OR order_status NOT IN ('created', 'approved', 'processing', 'shipped', 'delivered', 'canceled', 'unavailable'));
        """)
        rule8_fails = conn.execute(rule8_query).fetchall()
        if rule8_fails:
            print(f"RULE 8 FAILED: Có {len(rule8_fails)} đơn hàng mang trạng thái không tồn tại!")
            errors_found += 1
        else:
            print("RULE 8 PASSED: 100% đơn hàng nằm trong tập trạng thái hợp lệ.")

        # RULE 9: Ma trận Timestamp cho order_approved_at (Kiểm tra 2 chiều)
        rule9_query = text("""
            SELECT order_id, order_status
            FROM orders
            WHERE source_system = 'simulator'
              AND (
                  (order_status IN ('created', 'canceled', 'unavailable') AND order_approved_at IS NOT NULL)
                  OR
                  (order_status IN ('approved', 'processing', 'shipped', 'delivered') AND order_approved_at IS NULL)
              );
        """)
        rule9_fails = conn.execute(rule9_query).fetchall()
        if rule9_fails:
            print(f"RULE 9 FAILED: Có {len(rule9_fails)} đơn hàng vi phạm ma trận thời gian Duyệt đơn (Approved)!")
            errors_found += 1
        else:
            print("RULE 9 PASSED: Ma trận thời gian duyệt đơn chính xác 2 chiều.")

        # RULE 10: Ma trận Timestamp cho order_delivered_carrier_date (Kiểm tra 2 chiều)
        rule10_query = text("""
            SELECT order_id, order_status
            FROM orders
            WHERE source_system = 'simulator'
              AND (
                  (order_status IN ('created', 'approved', 'processing', 'canceled', 'unavailable') AND order_delivered_carrier_date IS NOT NULL)
                  OR
                  (order_status IN ('shipped', 'delivered') AND order_delivered_carrier_date IS NULL)
              );
        """)
        rule10_fails = conn.execute(rule10_query).fetchall()
        if rule10_fails:
            print(f"RULE 10 FAILED: Có {len(rule10_fails)} đơn hàng vi phạm ma trận thời gian Giao Vận (Carrier)!")
            errors_found += 1
        else:
            print("RULE 10 PASSED: Ma trận thời gian giao cho đơn vị vận chuyển chính xác 2 chiều.")

        # RULE 11: Ma trận Timestamp cho order_delivered_customer_date (Kiểm tra 2 chiều)
        rule11_query = text("""
            SELECT order_id, order_status
            FROM orders
            WHERE source_system = 'simulator'
              AND (
                  (order_status != 'delivered' AND order_delivered_customer_date IS NOT NULL)
                  OR
                  (order_status = 'delivered' AND order_delivered_customer_date IS NULL)
              );
        """)
        rule11_fails = conn.execute(rule11_query).fetchall()
        if rule11_fails:
            print(f"RULE 11 FAILED: Có {len(rule11_fails)} đơn hàng vi phạm ma trận thời gian Hoàn Thành (Customer)!")
            errors_found += 1
        else:
            print("RULE 11 PASSED: Ma trận thời gian giao tới khách hàng chính xác 2 chiều.")

        # RULE 12: Timestamp đúng trình tự nghiệp vụ + Bổ sung Technical Timestamp (updated_at)
        rule12_query = text("""
            SELECT order_id
            FROM orders
            WHERE source_system = 'simulator'
              AND (
                  (order_approved_at IS NOT NULL AND order_approved_at < order_purchase_timestamp) OR
                  (order_delivered_carrier_date IS NOT NULL AND order_delivered_carrier_date < order_approved_at) OR
                  (order_delivered_customer_date IS NOT NULL AND order_delivered_customer_date < order_delivered_carrier_date) OR
                  (updated_at < order_purchase_timestamp - INTERVAL '1 second')
              );
        """)
        rule12_fails = conn.execute(rule12_query).fetchall()
        if rule12_fails:
            print(f"RULE 12 FAILED: Phát hiện {len(rule12_fails)} đơn hàng vi phạm dòng thời gian vật lý (Time travel)!")
            errors_found += 1
        else:
            print("RULE 12 PASSED: 100% đơn hàng tuân thủ trình tự thời gian vật lý và kỹ thuật.")

        if errors_found == 0: 
            print("\nTất cả dữ liệu sinh ra từ simulator đều hợp lệ.")
        else:
            raise ValueError(f"\nSimulator validation thất bại: {errors_found} rule không đạt.")

if __name__ == "__main__":
    validate_simulator()