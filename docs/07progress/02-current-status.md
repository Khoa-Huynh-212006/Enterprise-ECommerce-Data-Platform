# Current Project Status

**As of:** 29/07/2026  
**Current phase:** Operational Database Bootstrap completed.

## Executive summary

FastOrder đã đi qua giai đoạn thiết kế và khởi tạo nguồn OLTP. Olist CSV đã được nạp thành công vào PostgreSQL thông qua schema do project kiểm soát. Project chưa bước vào simulator hoặc Airflow incremental ingestion.

## Completed

### Business and architecture

- Business context.
- Stakeholder/KPI mapping.
- Happy path và exception paths.
- Order state machine.
- High-level, logical và physical architecture.
- Data Flow Diagram.
- Source system strategy.
- Business entities và Data Dictionary bản đầu.

### Infrastructure

- Docker-based local environment.
- Airflow 3.3.0 với CeleryExecutor, Redis và metadata PostgreSQL.
- PostgreSQL 16 riêng cho FastOrder.
- FastOrder database sử dụng host port 5433.
- Olist CSV được lưu local tại `data/raw/olist/`.

### Python and database

- `fastorder/db/connection.py`
  - SQLAlchemy Core.
  - psycopg2 driver.
  - `URL.create()`.
  - Environment validation.
  - Shared Engine.
  - `SELECT 1` smoke test.
- `database/schema.sql`
  - 11 operational tables.
  - PK, FK, timestamps và indexes.
- `fastorder/db/init_db.py`
  - Development bootstrap/reset.
- `fastorder/ingestion/load_olist.py`
  - Atomic 9-table initial load.
  - FK-safe load order.
  - Rollback toàn bộ nếu một bảng lỗi.
- Olist data đã load thành công.

## Current database tables

- `geolocation`
- `customers`
- `warehouses`
- `orders`
- `products`
- `sellers`
- `order_items`
- `order_reviews`
- `order_payments`
- `product_category_name_translation`
- `inventory`

## Current limitations

- `warehouses` và `inventory` chưa được seed dữ liệu nghiệp vụ.
- Olist orders/order_items có thể chưa được gán `warehouse_id`.
- `updated_at` có default khi INSERT nhưng không tự đổi khi UPDATE; simulator phải cập nhật rõ ràng hoặc bổ sung trigger sau này.
- Initial loader không idempotent:
  - Chạy lần hai với `append` sẽ gặp duplicate PK.
  - Muốn chạy lại phải reset schema trước.
- Chưa có post-load reconciliation report.
- Chưa có Faker simulator.
- Chưa có Airflow incremental DAG.
- Chưa có Bronze/Silver/Gold implementation.

## Current phase gate

Không chuyển sang simulator cho đến khi hoàn thành:

1. Đối chiếu row count CSV và PostgreSQL.
2. Kiểm tra orphan FK bằng SQL.
3. Kiểm tra NULL và timestamp parsing.
4. Ghi kết quả vào progress docs.
