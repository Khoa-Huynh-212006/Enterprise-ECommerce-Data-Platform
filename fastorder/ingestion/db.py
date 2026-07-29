# Kết nối Python -> PostgreSQL
import psycopg2
import os
from psycopg2 import OperationalError
from dotenv import load_dotenv

#Tự động tìm và load các biến môi trường trong .env
load_dotenv()

def get_connection():
    """
    Tạo kết nối đến cơ sở dữ liệu PostgreSQL
    """
    try: 
        connection = psycopg2.connect(
            host = os.getenv("DB_HOST"),
            port = os.getenv("DB_PORT"),
            database = os.getenv("DB_NAME"),
            user = os.getenv("DB_USER"),
            password = os.getenv("DB_PASSWORD")
        )
        return connection
    except OperationalError as e:
        print(f"Error: {e}")
        return None

if __name__ == "__main__": 
    print("Đang thử kết nối tới database ...")
    try:
        conn = get_connection()

        if conn is not None:
            print("Kết nối thành công!")

            #Lấy thông tin về phiên bản PostgreSQL
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            print(f"Phiên bản PostgreSQL: {version[0]}")

            cursor.close()
            conn.close()
            
        else:
            print("Kết nối thất bại.")
    except Exception as e:
        print(f"Đã xảy ra lỗi: {e}")
