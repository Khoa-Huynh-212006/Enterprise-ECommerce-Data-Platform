from fastorder.db.connection import get_engine
from sqlalchemy import text


def seed_warehouses():
    engine = get_engine()
    dict_warehouses = [
            {"warehouse_id": "WH_HN", "warehouse_city": "Hà Nội", "warehouse_region": "NORTH"},
            {"warehouse_id": "WH_HP", "warehouse_city": "Hải Phòng", "warehouse_region": "NORTH"},
            {"warehouse_id": "WH_DN", "warehouse_city": "Đà Nẵng", "warehouse_region": "CENTRAL"},
            {"warehouse_id": "WH_HCM", "warehouse_city": "TP.Hồ Chí Minh", "warehouse_region": "SOUTH"},
            {"warehouse_id": "WH_CT", "warehouse_city": "Cần Thơ", "warehouse_region": "SOUTH"}
        ]
    query = text("""
        INSERT INTO warehouses(warehouse_id, warehouse_city, warehouse_region) 
        VALUES(:warehouse_id, :warehouse_city, :warehouse_region)
        ON CONFLICT (warehouse_id)
        DO UPDATE SET
            warehouse_city = EXCLUDED.warehouse_city,
            warehouse_region = EXCLUDED.warehouse_region,
            updated_at = CURRENT_TIMESTAMP; 
        """)

    count_query = text("""
        SELECT COUNT(*) 
        FROM warehouses 
        WHERE warehouse_id 
        IN (  
            'WH_HN',
            'WH_HP',
            'WH_DN',
            'WH_HCM',
            'WH_CT'
        )
        """)
    
    with engine.begin() as conn:
        conn.execute(query, dict_warehouses)
        count = conn.execute(count_query).scalar_one()

        if count != 5:
            raise ValueError(f"Nhập dữ liệu thông vào cho các warehouses đã thất bại")
        else:
            print(f"Dữ liệu thông tin của cả  5 warehouse đã được thêm vào thành công")
        
if __name__ == "__main__":
    print("Bắt đầu seed dữ liệu 5 Warehouse...")
    seed_warehouses()