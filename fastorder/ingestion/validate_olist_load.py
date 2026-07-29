from pathlib import Path
from sqlalchemy import text 
from fastorder.db.connection import get_engine
import pandas as pd 

def validate():

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
    failed = False
    with engine.connect() as conn:
        for file_csv, table_in_database in entity_dict_mapping.items():
            file_path = root_path / "data" / "raw" / "olist" / file_csv
            if not file_path.exists():
                print(f"Không tìm thấy file trong đường dẫn {file_path}")
                failed = True
                continue

            df = pd.read_csv(file_path)
            csv_count = len(df)

            query = text(f"SELECT COUNT(*) FROM {table_in_database}")
            result_query = conn.execute(query)
            db_count = result_query.scalar_one()

            if csv_count != db_count:
                failed = True
    if failed: 
        raise ValueError("Kiểm tra toàn vẹn dữ liệu thất bại. Phát hiện chênh lệch dữ liệu giữa CSV và Database")
    print("Quá trình validation thành công. Toàn bộ dữ liệu khớp nhau 100%.")

if __name__ == "__main__":
    print("Bắt đầu tiến trình Validation (Kiểm đếm dữ liệu)...")
    validate()


            

    
