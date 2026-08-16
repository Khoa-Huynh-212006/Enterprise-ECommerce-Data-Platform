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

### Flow 1 — Operational Database
PostgreSQL → Timestamp Incremental Ingestion → Airflow → ADLS Bronze
**Status:** COMPLETE

### Flow 2 — File-Based Source
External YOOCHOOSE → ADF → ADLS Landing → Databricks → File Discovery → Manifest → Runner → Bronze Writer
**Status:** CODE COMPLETE 
*(Cần thiết lập Airflow DAG và chạy validation chính thức)*

### Flow 3 — External API Source (Weather)
Open-Meteo Forecast & Historical → HTTP Client → UUID5 Identity → Bronze Sidecar Pattern
**Status:** COMPLETE

**Validated:**
- E2E Weather Forecast (Incremental 48h).
- E2E Historical Forecast (Bootstrap 90 days, 30-day windows).
- Raw JSON + Metadata sidecar preservation.
- Deterministic UUID5 paths.
- `_SUCCESS` commit marker protocol.
- 2-Layer Retry (HTTP backoff + Airflow retry).

### Not Started

- Bronze → Silver
- Synapse
- dbt Gold
- Power BI