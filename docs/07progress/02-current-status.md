# Current Project Status

**As of:** 2026-08-10
**Current phase:** ADLS Bronze Integration (Next Phase)

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
- Post-load row-count validation hoàn tất thành công.

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

- Olist orders/order_items có thể chưa được gán `warehouse_id`[cite: 3].
- `updated_at` có default khi INSERT nhưng không tự đổi khi UPDATE; simulator phải cập nhật rõ ràng hoặc bổ sung trigger sau này[cite: 3].
- Initial loader không idempotent:
  - Chạy lần hai với `append` sẽ gặp duplicate PK[cite: 3].
  - Muốn chạy lại phải reset schema trước[cite: 3].
- Chưa có Airflow incremental DAG[cite: 3].
- Chưa có Bronze/Silver/Gold implementation[cite: 3].

## Current phase gate
Giai đoạn Local Incremental Ingestion MVP (PostgreSQL -> Local Bronze qua Airflow) đã chính thức hoàn thành toàn diện. Kiến trúc đã chứng minh được tính ổn định, tự phục hồi và bảo toàn dữ liệu 100%.

Các hạng mục đã hoàn tất:
10. Chốt Orders Incremental Source Contract - COMPLETE
11. Xây dựng Checkpoint Manager (Atomic JSON File) - COMPLETE
12. Design and implement Orders Incremental Extractor - COMPLETE
13. Xây dựng Bronze Parquet Writer (Idempotent atomic write) - COMPLETE
14. Thiết kế Incremental Runner (Multi-batch, Pending Context, Crash Recovery) - COMPLETE
15. Local End-to-End Operational Validation - COMPLETE
16. Chuyển đổi và cấu hình Airflow DAG cho Incremental Runner - COMPLETE

Mục tiêu tiếp theo (Next Phase):
17. Tích hợp Azure Data Lake Storage (ADLS Gen2) cho Bronze Layer.
18. Thay thế logic ghi file Parquet từ Local Storage sang ADLS (Giữ nguyên toàn bộ logic Orchestration, Extractor và Checkpoint).

