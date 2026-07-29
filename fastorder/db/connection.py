# Kết nối Python -> PostgreSQL
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy import text
import os
from dotenv import load_dotenv

#Tự động tìm và load các biến môi trường trong .env
load_dotenv()

# Biến toàn cục
_engine = None

def get_engine():
    """
    Khởi tạo và trả về SQLAlchemy Engine.
    """
    global _engine 
    if _engine is None:
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")
        database = os.getenv("DB_NAME")

        if port == "":
            port = None

        env_dict = {
            "DB_USER": user,
            "DB_PASSWORD": password,
            "DB_HOST": host,
            "DB_PORT": port,
            "DB_NAME": database
        }

        for key, value in env_dict.items():
            if value is None or value.strip() == "":
                raise ValueError(f"Biến môi trường {key} chưa được thiết lập. Vui lòng kiểm tra file .env\n")

        db_url = URL.create(
            drivername="postgresql+psycopg2",
            username=user,
            password=password,
            host=host,
            port=int(port),
            database=database
        )
        _engine = create_engine(db_url)
        print("Đã khởi tạo SQLAlchemy Engine thành công")
    return _engine

if __name__ == "__main__":
    print("Đang kiểm tra kết nối cơ sở dữ liệu...")
    try: 
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            if result.scalar() == 1:
                print("Kết nối cơ sở dữ liệu thành công!")
    except Exception as e:  
        print(f"Không thể kết nối cơ sở dữ liệu: {e}") 