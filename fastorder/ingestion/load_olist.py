from fastorder.db.connection import get_engine
import pandas as pd
from pathlib import Path



def load_data():
    entity_dict_mapping = {
        "olist_geolocation_dataset.csv":"geolocation",
        "product_category_name_translation.csv":"product_category_name_translation",
        "olist_customers_dataset.csv": "customers",
        "olist_products_dataset.csv":"products",
        "olist_sellers_dataset.csv":"sellers",
        "olist_orders_dataset.csv":"orders",
        "olist_order_items_dataset.csv":"order_items",
        "olist_order_payments_dataset.csv":"order_payments",
        "olist_order_reviews_dataset.csv":"order_reviews"
    }

    engine = get_engine()
    root_path = Path(__file__).resolve().parents[2]

    
    for file_name, entity in entity_dict_mapping.items():
        file_path = root_path / "data" / "raw" / "olist" / file_name
        if not file_path.exists():
            raise FileNotFoundError(f"file {file_name} không tồn tại trong đường dẫn {file_path}")
    print("Đã tìm thấy đủ files, tiến hành tiến trình ghi file vào database")
    try:
        with engine.begin() as conn:

            for file_name, entity in entity_dict_mapping.items():
                file_path = root_path / "data" / "raw" / "olist" / file_name

                print(f"Đang ghi dữ liệu {file_name} thành DataFrame")
                df = pd.read_csv(file_path)
                if entity == "orders": 
                    df['source_system'] = 'olist_seed'
                print(f"Đã ghi xong dữ liệu {file_name} vào DataFrame với {len(df)} dòng dữ liệu")

                print(f"Tiến hành đẩy dữ liệu vào Database PostgreSQL")
                df.to_sql(
                    name = entity, 
                    con = conn,
                    if_exists= "append",
                    index= False,
                    chunksize= 2500,
                    method = "multi"
                )
        print("Tất cả dữ liệu từ file csv đã được load thành công vào database")
    except Exception as e:
        print(f"Có lỗi nghiêm trọng, ĐÃ TỰ ĐỘNG ROLLBACK toàn bộ 9 bảng. Chi tiết: {e}")
        raise #để airflow phát hiện ra lỗi


if __name__ == "__main__":
    print("Bắt đầu tiến trình load dữ liệu từ các file olist csv vào Database")
    load_data()




