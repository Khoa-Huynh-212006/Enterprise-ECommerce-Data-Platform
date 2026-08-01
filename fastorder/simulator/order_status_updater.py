import random
from sqlalchemy import text
from fastorder.db.connection import get_engine

TERMINAL_STATUSES = {"delivered", "canceled", "unavailable"}

STATUS_TRANSITIONS = {
    "created": [
        {"next_status": "approved", "weight": 90},
        {"next_status": "canceled", "weight": 8},
        {"next_status": "unavailable", "weight": 2}
    ],
    "approved": [
        {"next_status": "processing", "weight": 100}
    ],
    "processing": [
        {"next_status": "shipped", "weight": 100}
    ],
    "shipped": [
        {"next_status": "delivered", "weight": 100}
    ]
}

def get_next_status(current_status: str) -> str | None:
    if current_status in TERMINAL_STATUSES:
        return None

    if current_status not in STATUS_TRANSITIONS:
        raise ValueError(f"Order status không hợp lệ: {current_status}")

    transitions = STATUS_TRANSITIONS[current_status]
    possible_statuses = [t["next_status"] for t in transitions]
    weights = [t["weight"] for t in transitions]

    return random.choices(possible_statuses, weights=weights, k=1)[0]

def order_status_updater() -> dict:
    """
    Quét và cập nhật trạng thái đơn hàng.
    Trả về dictionary chứa các metrics để Runner tổng hợp.
    """
    engine = get_engine()
    nums_order_will_update = random.randint(1, 20)
    
    summary = {
        "selected_orders": 0,
        "updated_orders": 0,
        "transition_counts": {}
    }
    
    with engine.begin() as conn:
       
        orders_valid_query = text("""
            SELECT order_id, order_status
            FROM orders
            WHERE source_system = 'simulator' 
              AND order_status IN ('created','approved','processing','shipped')
              AND (
                  (order_status = 'created'    AND updated_at <= CURRENT_TIMESTAMP - INTERVAL '2 minutes') OR
                  (order_status = 'approved'   AND updated_at <= CURRENT_TIMESTAMP - INTERVAL '15 minutes') OR
                  (order_status = 'processing' AND updated_at <= CURRENT_TIMESTAMP - INTERVAL '2 hours') OR
                  (order_status = 'shipped'    AND updated_at <= CURRENT_TIMESTAMP - INTERVAL '1 days')
              )
            ORDER BY updated_at ASC
            LIMIT :limit_num
            FOR UPDATE SKIP LOCKED
        """)

        results = conn.execute(orders_valid_query, {"limit_num": nums_order_will_update}).fetchall()
        
        if not results:
            print("Không có order nào đang chờ update status (hoặc chưa đủ thời gian chờ nghiệp vụ).")
            return summary 

        summary["selected_orders"] = len(results)
        print(f"Tiến hành cập nhật trạng thái cho {len(results)} đơn hàng...")

        for row in results: 
            order_id, current_status = row
            next_status = get_next_status(current_status)

            if not next_status:
                continue

           
            if next_status in ("canceled", "unavailable"):
                count_query = text("""
                    SELECT COUNT(*) FROM (
                        SELECT warehouse_id, product_id
                        FROM order_items
                        WHERE order_id = :order_id
                        GROUP BY warehouse_id, product_id
                    ) AS expected_inventory;
                """)
                expected_count = conn.execute(count_query, {"order_id": order_id}).scalar()
                
                restock_query = text("""
                    UPDATE inventory i
                    SET quantity_available = i.quantity_available + restored.total_quantity,
                        updated_at = CURRENT_TIMESTAMP
                    FROM (
                        SELECT
                            warehouse_id,
                        product_id,
                        SUM(quantity) AS total_quantity
                        FROM order_items
                        WHERE order_id = :order_id
                        GROUP BY warehouse_id, product_id
                    ) AS restored
                    WHERE i.warehouse_id = restored.warehouse_id
                      AND i.product_id = restored.product_id;
                """)
                restock_result = conn.execute(restock_query, {"order_id": order_id})
                
                if restock_result.rowcount != expected_count:
                    raise ValueError(
                        f"Inventory Restock Mismatch for order {order_id}: "
                        f"Expected {expected_count} inventory records to update, "
                        f"but actually updated {restock_result.rowcount}."
                    )
                print(f"  -> Đã hoàn trả tồn kho thành công cho đơn {order_id[:8]}...")

            update_params = {
                "order_id": order_id,
                "current_status": current_status,
                "next_status": next_status
            }

            set_clauses = [
                "order_status = :next_status",
                "updated_at = CURRENT_TIMESTAMP"
            ]
            
            if next_status == 'approved':
                set_clauses.append("order_approved_at = CURRENT_TIMESTAMP")
            elif next_status == 'shipped':
                set_clauses.append("order_delivered_carrier_date = CURRENT_TIMESTAMP")
            elif next_status == 'delivered':
                set_clauses.append("order_delivered_customer_date = CURRENT_TIMESTAMP")
                
            update_query = text(f"""
                UPDATE orders
                SET {', '.join(set_clauses)}
                WHERE order_id = :order_id
                  AND source_system = 'simulator'
                  AND order_status = :current_status
            """)
            
            result = conn.execute(update_query, update_params)
            
            if result.rowcount != 1:
                raise ValueError(
                    f"Concurrency Conflict / State Drift detected for order {order_id}. "
                    f"Expected rowcount 1, got {result.rowcount}."
                )
            
            summary["updated_orders"] += 1
            transition_key = f"{current_status}->{next_status}"
            summary["transition_counts"][transition_key] = summary["transition_counts"].get(transition_key, 0) + 1
                
            print(f"  -> Order {order_id[:8]}... : {current_status} => {next_status}")

    return summary

if __name__ == "__main__":
    print("BẮT ĐẦU TIẾN TRÌNH CẬP NHẬT TRẠNG THÁI ĐƠN HÀNG")
    result = order_status_updater()
    print("\nKết quả thực thi:")
    print(result)