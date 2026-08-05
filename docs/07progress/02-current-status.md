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
Không chuyển sang Airflow DAG cho đến khi hoàn thành:
1. Đối chiếu row count CSV và PostgreSQL - **COMPLETE**
2. Kiểm tra orphan FK bằng SQL - **COMPLETE**
3. Kiểm tra NULL ở các cột bắt buộc - **COMPLETE**
4. Warehouse design and seed - **COMPLETE**
5. Inventory design and seed - **COMPLETE**
6. Inventory validation - **COMPLETE**
7. Faker simulator design (State machine, Quantity, Payment logic) - **COMPLETE**
8. Faker simulator implementation (CREATE_ORDER event) - **COMPLETE**
9. Mở rộng Simulator chạy Batch (Runner Bounded & Continuous mode) - **COMPLETE** 
10. Chốt Orders Incremental Source Contract (Composite watermark, Not Null enforcement, Naive Timestamp format) - **COMPLETE**
11. Xây dựng Checkpoint Manager (Atomic JSON File) - **NEXT**
12. Xây dựng PostgreSQL Incremental Extractor (Python logic) - **PENDING**

## Trạng thái hiện tại (Tính đến 31/07/2026)

*   **Giai đoạn:** Data Quality & Validation (Hoàn tất).
*   **Tình trạng hệ thống:** Cỗ máy Simulator (gồm Generator và Updater) đã hoạt động trơn tru, sinh dữ liệu chuẩn xác về mặt tài chính (Reconciliation), không gian (Kho bãi) và logic thời gian vật lý (Time-series).
*   **Thành tựu cốt lõi:** Chốt chặt 12 quy tắc Data Invariants thông qua phương pháp Negative Testing bằng SQL, đảm bảo không có rác dữ liệu hay nghịch lý thời gian trong quá trình giả lập.
*   **Mục tiêu tiếp theo:** Chuẩn bị hạ tầng để đưa dòng dữ liệu giả lập này chảy vào các hệ thống Data Pipeline thực thụ (như Kafka, Airflow, hoặc Data Warehouse).

