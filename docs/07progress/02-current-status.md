# Current Project Status

**As of:** 07/08/2026
**Current phase:** Incremental Runner & Crash Recovery COMPLETE.

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

Hệ thống Local Bronze Layer (PostgreSQL -> Parquet) đã hoàn thành toàn bộ cốt lõi. Sẵn sàng chuyển sang giai đoạn đóng gói Airflow Orchestration và chuyển đổi sang Azure Data Lake Storage (ADLS).


1. Đối chiếu row count CSV và PostgreSQL - **COMPLETE**
2. Kiểm tra orphan FK bằng SQL - **COMPLETE**
3. Kiểm tra NULL ở các cột bắt buộc - **COMPLETE**
4. Warehouse design and seed - **COMPLETE**
5. Inventory design and seed - **COMPLETE**
6. Inventory validation - **COMPLETE**
7. Faker simulator design (State machine, Quantity, Payment logic) - **COMPLETE**
8. Faker simulator implementation (CREATE_ORDER event) - **COMPLETE**
9. Mở rộng Simulator chạy Batch (Runner Bounded & Continuous mode) - **COMPLETE** 
10. Chốt Orders Incremental Source Contract (Composite watermark, Not Null enforcement) - **COMPLETE**
11. Xây dựng Checkpoint Manager (Atomic JSON File) - **COMPLETE**
12. Design and implement Orders Incremental Extractor - **COMPLETE**
13. Xây dựng Bronze Parquet Writer (Idempotent atomic write, Metadata injection) - **COMPLETE**
14. Thiết kế Incremental Runner (Multi-batch, Pending Context, Crash Recovery) - **COMPLETE**
Mục tiêu tiếp theo (Next Phase):
15. Đưa Ingestion Runner lên Airflow DAG.
16. Tích hợp ADLS Gen2 để thay thế Local Path.




