# Local Development Runbook

**Cập nhật:** 29/07/2026

## 1. Start infrastructure

Từ repository root:

```bash
docker compose up -d
```

Xác nhận Airflow, Redis, Airflow metadata PostgreSQL và FastOrder PostgreSQL healthy.

## 2. Required environment variables

```env
DB_USER=
DB_PASSWORD=
DB_HOST=localhost
DB_PORT=5433
DB_NAME=fastorder
```

Không commit `.env` thật. Duy trì `.env.example`.

## 3. Test FastOrder database connection

```bash
python -m fastorder.db.connection
```

Expected:

```text
Đã khởi tạo SQLAlchemy Engine thành công
Kết nối cơ sở dữ liệu thành công
```

## 4. Reset/create schema

```bash
python -m fastorder.db.init_db
```

> Cảnh báo: `schema.sql` có `DROP TABLE`. Lệnh này xóa dữ liệu nghiệp vụ hiện tại và dựng lại schema development.

## 5. Initial load

```bash
python -m fastorder.ingestion.load_olist
```

Loader kiểm tra đủ file, sau đó nạp 9 bảng trong một transaction.

## 6. Verify

Kiểm tra:

```sql
SELECT COUNT(*) FROM customers;
SELECT COUNT(*) FROM orders;
SELECT COUNT(*) FROM order_items;
SELECT COUNT(*) FROM products;
SELECT COUNT(*) FROM sellers;
SELECT COUNT(*) FROM order_payments;
SELECT COUNT(*) FROM order_reviews;
SELECT COUNT(*) FROM geolocation;
SELECT COUNT(*) FROM product_category_name_translation;
```

## 7. Common failures

### `IntegrityError` / SQLAlchemy `gkpj`

Không đọc dòng link cuối như nguyên nhân. Tìm phần:

```text
psycopg2.errors...
DETAIL: ...
```

### Duplicate primary key khi chạy loader

Nguyên nhân thường là chạy loader lần hai với `append`.

Resolution:

```bash
python -m fastorder.db.init_db
python -m fastorder.ingestion.load_olist
```

### Geolocation unique violation

Đã giải quyết bằng surrogate primary key. Không thêm `drop_duplicates()` âm thầm vào loader.

### Wrong database

Kiểm tra `DB_PORT`:

- 5432: Airflow metadata DB.
- 5433: FastOrder operational DB.

## 8. Shutdown

```bash
docker compose down
```

Không dùng `-v` nếu không muốn xóa volume.
