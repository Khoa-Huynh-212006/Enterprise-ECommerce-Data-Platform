from pathlib import Path
from sqlalchemy import text 
from fastorder.db.connection import get_engine
import pandas as pd 


def validate_row_counts(conn, root_path):
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
    failed = False
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
            status = "FAIL"
        else:
            status = "PASS"
        print(f"{table_in_database:<35} | CSV: {csv_count:<7} | DB: {db_count:<7} | {status}")
    if failed: 
        print("Row-count validation thất bại. Có sự chênh lệch số lượng dòng")
    else:
        print("Row-count validation thành công: số dòng của các bảng khớp với các file CSV")

    return failed

def validate_foreign_keys(conn):
    fk_checks = {
        "orders -> customers": """
            SELECT COUNT(*) FROM orders o 
            LEFT JOIN customers c ON o.customer_id = c.customer_id 
            WHERE c.customer_id IS NULL;
        """,
        "order_items -> orders": """
            SELECT COUNT(*) FROM order_items oi 
            LEFT JOIN orders o ON oi.order_id = o.order_id 
            WHERE o.order_id IS NULL;
        """,
        "order_items -> products": """
            SELECT COUNT(*) FROM order_items oi 
            LEFT JOIN products p ON oi.product_id = p.product_id 
            WHERE p.product_id IS NULL;
        """,
        "order_items -> sellers": """
            SELECT COUNT(*) FROM order_items oi 
            LEFT JOIN sellers s ON oi.seller_id = s.seller_id 
            WHERE s.seller_id IS NULL;
        """,
        "order_payments -> orders": """
            SELECT COUNT(*) FROM order_payments op 
            LEFT JOIN orders o ON op.order_id = o.order_id 
            WHERE o.order_id IS NULL;
        """,
        "order_reviews -> orders": """
            SELECT COUNT(*) FROM order_reviews orv 
            LEFT JOIN orders o ON orv.order_id = o.order_id 
            WHERE o.order_id IS NULL;
        """
    }
    
    failed = False
    
    for check_name, sql_query in fk_checks.items():
        query = text(sql_query)
        orphan_count = conn.execute(query).scalar_one()
        
        if orphan_count == 0:
            status = "PASS"
        else:
            status = "FAIL"
            failed = True
            
        print(f"{check_name:<30} : {orphan_count:<5} orphan | {status}")
        
    if failed:
        print("Foreign-key validation thất bại")
    else:
        print("Tất cả foreign key relationship đều hợp lệ.")
    return failed

def validate_required_nulls(conn):
    failed = False 
    required_col = {
        "customers": ["customer_unique_id"],
        "orders": ["customer_id", "order_status", "order_purchase_timestamp"],
        "order_items": ["order_id", "order_item_id", "product_id", "seller_id", "price", "freight_value"],
        "order_payments": ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"],
        "order_reviews": ["review_id", "order_id"]
    }

    for table, columns in required_col.items(): 
        for column in columns:
            query = text(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL")
            result = conn.execute(query)
            null_count = result.scalar_one()

            if null_count == 0:
                status = "PASS"
            else:
                failed = True
                status = "FAIL"
            col_name = f"{table}.{column}"
            print(f"{col_name:<40} | NULL: {null_count:<5} | {status}")
    if failed:
        print("NULL validation thất bại. Phát hiện giá trị NULL ở các cột bắt buộc")
    else:
        print("Tất cả các cột bắt buộc đều không chứa giá trị NULL")
    return failed
def validate():
    engine = get_engine()
    root_path = Path(__file__).resolve().parents[2]
    
    with engine.connect() as conn:
        row_count_failed = validate_row_counts(conn, root_path)
        fk_failed = validate_foreign_keys(conn)
        null_failed = validate_required_nulls(conn)
        
        if row_count_failed or fk_failed or null_failed:
            raise ValueError("Kiểm tra toàn vẹn dữ liệu thất bại")
        else:
            print("THÀNH CÔNG: Toàn bộ post-load validation hiện tại thành công.")
        print("="*60 + "\n")

if __name__ == "__main__":
    print("Bắt đầu tiến trình Validation (Kiểm đếm và Toàn vẹn Dữ liệu)...")
    validate()  

    
