# Current Project Status
**As of:** 2026-08-11
**Current phase:** Silver Layer Integration (Apache Spark)
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
Giai đoạn Local Airflow Incremental Ingestion MVP đã chính thức đóng lại với kết quả PASS toàn bộ các bài test E2E. Hệ thống đã sẵn sàng để thay thế Local Storage bằng Cloud Storage.

Các hạng mục đã hoàn tất:
13. Xây dựng Bronze Parquet Writer (Idempotent atomic write) - COMPLETE
14. Thiết kế Incremental Runner (Multi-batch, Pending Context, Crash Recovery) - COMPLETE
15. Local End-to-End Operational Validation - COMPLETE
16. Chuyển đổi và cấu hình Airflow DAG cho Incremental Runner - COMPLETE
17. Khởi tạo `adls_client.py` và thực hiện Connection Probe (Authentication & Base I/O) - COMPLETE
18. Tích hợp ADLS Gen2 Client vào Bronze Writer (Thay thế Local Path) - COMPLETE
19. Crash Recovery Testing với Storage Mây (Crash after ADLS, Crash after Checkpoint) - COMPLETE
20. Airflow E2E Validation và Fix Timezone bug - COMPLETE

Mục tiêu tiếp theo (Next Phase):
21. Thiết lập môi trường Apache Spark (Local/Containerized).
22. Xây dựng Data Quality Checks cho quá trình di chuyển dữ liệu từ Bronze sang Silver.
23. Thiết kế bảng Orders Silver với ACID propertie