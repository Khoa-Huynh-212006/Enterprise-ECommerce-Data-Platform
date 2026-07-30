import random
from sqlalchemy import text
from fastorder.db.connection import get_engine

def seed_inventory():
    engine = get_engine()
    
    random.seed(42)

    with engine.begin() as conn:
        products_query = text("SELECT product_id FROM products ORDER BY product_id;")
        products = [row[0] for row in conn.execute(products_query)]
        
        if not products:
            raise ValueError("Không tìm thấy product nào. Hãy đảm bảo initial load đã chạy thành công.")

        main_whs = ['WH_HN', 'WH_HCM']
        regional_whs = ['WH_HP', 'WH_DN', 'WH_CT']
        all_whs = main_whs + regional_whs

        inventory_records = []

        for product_id in products:
            main = random.choice(main_whs)
            inventory_records.append({
                "warehouse_id": main,
                "product_id": product_id,
                "quantity_available": random.randint(50, 200)
            })

            regional = random.choice(regional_whs)
            inventory_records.append({
                "warehouse_id": regional,
                "product_id": product_id,
                "quantity_available": random.randint(20, 100)
            })

            if random.random() < 0.20:
                remaining_whs = [wh for wh in all_whs if wh not in {main, regional}]
                third = random.choice(remaining_whs)
                inventory_records.append({
                    "warehouse_id": third,
                    "product_id": product_id,
                    "quantity_available": random.randint(10, 50)
                })

        insert_query = text("""
            INSERT INTO inventory (warehouse_id, product_id, quantity_available)
            VALUES (:warehouse_id, :product_id, :quantity_available)
            ON CONFLICT (warehouse_id, product_id) 
            DO NOTHING
        """)

        count_query = text("""
            SELECT COUNT(*)
            FROM inventory
        """)
        count_before = conn.execute(count_query).scalar_one()
        conn.execute(insert_query, inventory_records)
        count_after = conn.execute(count_query).scalar_one()
        print(f"Đã sinh {len(inventory_records)} inventory records để seed.")
        print(f"Đã insert mới {count_after - count_before} bản ghi.")
        print(f"Tổng số bản ghi hiện có trong inventory: {count_after}")

    with engine.connect() as conn:
        print("\n--- Báo cáo phân phối nhà kho ---")
        dist_query = text("""
            SELECT warehouse_id, COUNT(product_id), SUM(quantity_available) 
            FROM inventory 
            GROUP BY warehouse_id 
            ORDER BY warehouse_id;
        """)
        
        print(f"{'Warehouse':<10} | {'Products':>10} | {'Total Quantity':>15}")
        print("-" * 42)
        for row in conn.execute(dist_query):
            print(f"{row[0]:<10} | {row[1]:>10,} | {row[2]:>15,}")
        print("-" * 42)

if __name__ == "__main__":
    print("Bắt đầu seed dữ liệu Inventory...")
    seed_inventory()