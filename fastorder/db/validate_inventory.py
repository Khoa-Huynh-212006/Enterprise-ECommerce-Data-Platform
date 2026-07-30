from fastorder.db.connection import get_engine
from sqlalchemy import text 

def validate_inventory():
    engine = get_engine()
    failed = False
    
    # Kiểm tra bảng rỗng
    query_empty_chk = text("SELECT COUNT(*) FROM inventory")

    # Rule 1a: Mỗi product có ít nhất 2 kho 
    query_product_min_chk = text("""
        SELECT COUNT(*)
        FROM (
            SELECT p.product_id
            FROM products p
            LEFT JOIN inventory i ON p.product_id = i.product_id
            GROUP BY p.product_id
            HAVING COUNT(i.warehouse_id) < 2
        ) AS sub_query
    """)

    # Rule 1b: Mỗi product có tối đa 3 kho 
    query_product_max_chk = text("""
        SELECT COUNT(*)
        FROM (
            SELECT product_id
            FROM inventory
            GROUP BY product_id
            HAVING COUNT(warehouse_id) > 3
        ) AS max_check
    """)

    # Rule 2: Đảm bảo không có kho nào bị trống 
    query_warehouse_chk = text("""
        WITH required_warehouses(warehouse_id) AS (
            VALUES ('WH_HN'), ('WH_HP'), ('WH_DN'), ('WH_HCM'), ('WH_CT')
        )
        SELECT COUNT(*)
        FROM required_warehouses r
        LEFT JOIN inventory i ON r.warehouse_id = i.warehouse_id
        WHERE i.warehouse_id IS NULL;
    """)

    # Rule 3: Số lượng tồn kho không được phép nhỏ hơn 0
    query_quantity_chk = text("""
        SELECT count(*)
        FROM inventory 
        WHERE quantity_available < 0
    """)

    # Rule 4: Không được phép trùng PK 
    query_duplicate_inventory_chk = text("""
        SELECT COUNT(*)
        FROM (
            SELECT warehouse_id, product_id
            FROM inventory
            GROUP BY warehouse_id, product_id
            HAVING COUNT(*) > 1
        ) AS duplicate_check
    """)

    with engine.connect() as conn:
        print("Tiến hành kiểm tra seed_inventory")
        
        # 0. Prerequisite Check
        total_rows = conn.execute(query_empty_chk).scalar_one()
        if total_rows == 0:
            raise ValueError("Prerequisite thất bại: Bảng inventory rỗng. Hãy chạy seed_inventory.py trước")
        else:
            print(f"Prerequisite (Bảng có dữ liệu): PASS ({total_rows} records)")

        # 1a. Min 2 kho
        cnt_min = conn.execute(query_product_min_chk).scalar_one()
        status_1a = "PASS" if cnt_min == 0 else "FAIL"
        print(f"Rule 1a (Mỗi product >= 2 kho): {cnt_min:<5} lỗi | {status_1a}")
        if cnt_min > 0: failed = True
            
        # 1b. Max 3 kho
        cnt_max = conn.execute(query_product_max_chk).scalar_one()
        status_1b = "PASS" if cnt_max == 0 else "FAIL"
        print(f"Rule 1b (Mỗi product <= 3 kho): {cnt_max:<5} lỗi | {status_1b}")
        if cnt_max > 0: failed = True

        # 2. 5 kho đều có hàng
        cnt_wh = conn.execute(query_warehouse_chk).scalar_one()
        status_2 = "PASS" if cnt_wh == 0 else "FAIL"
        print(f"Rule 2  (5 kho đều có hàng):{cnt_wh:<5} lỗi | {status_2}")
        if cnt_wh > 0: failed = True
            
        # 3. Tồn kho >= 0
        cnt_qty = conn.execute(query_quantity_chk).scalar_one()
        status_3 = "PASS" if cnt_qty == 0 else "FAIL"
        print(f"Rule 3  (Tồn kho >= 0):{cnt_qty:<5} lỗi | {status_3}")
        if cnt_qty > 0: failed = True
            
        # 4. Duplicate PK
        cnt_dup = conn.execute(query_duplicate_inventory_chk).scalar_one()
        status_4 = "PASS" if cnt_dup == 0 else "FAIL"
        print(f"Rule 4  (Không trùng lặp PK):{cnt_dup:<5} lỗi | {status_4}")
        if cnt_dup > 0: failed = True

    if failed:
        raise ValueError("Inventory Validation thất bại. Phát hiện dữ liệu không thỏa mãn Business Rule")
    else:
        print("Toàn bộ dữ liệu Inventory đều hợp lệ.")

if __name__ == "__main__":
    validate_inventory()