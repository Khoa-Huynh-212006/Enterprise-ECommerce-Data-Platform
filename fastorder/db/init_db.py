from fastorder.db.connection import get_engine
from pathlib import Path 
from sqlalchemy import text 

def init_database():
    print("Đọc file sql để khởi tạo database")
    root = Path(__file__).resolve().parents[2]
    file_path = root / "database" / "schema.sql"

    if not file_path.exists():
        print(f"Đường dẫn tới file sql là {file_path} không tồn tại")
        return 

    print("Đang đọc file cấu trúc schema.sql")
    with open(file_path, "r", encoding= "utf-8") as file_encoded:
        sql_script = file_encoded.read()

    if not sql_script.strip():
        print("file schema.sql hiện tại đang không chứa SQL")
        return

    print("Đang khởi tạo database ...")
    engine = get_engine()
    try: 
        with engine.begin() as conn:
            conn.execute(text(sql_script))
        print("Khởi tạo database thành công")
    except Exception as e:
        print(f"Quá trình chạy file SQL thất bại, chi tiết lỗi {e}")

if __name__ == "__main__":
    print("Bắt đầu khởi tạo database")
    init_database()









