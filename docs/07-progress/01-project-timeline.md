# Project Timeline — FastOrder Enterprise E-Commerce Data Platform

**Last updated:** 31/08/2026  
**Current phase:** Local-First Architecture Migration

> Timeline là **lịch sử**. Các mốc Azure/Databricks bên dưới phản ánh kiến trúc tại thời điểm chúng được thực hiện; current architecture phải đọc từ `02-current-status.md` và `../02-architecture/03-physical-architecture.md`.

---

## Phase 1 — Foundation & Business Design (23–29/07/2026)

### Mốc 1 — Local Airflow Foundation — COMPLETE

- Airflow 3.3.0 chạy bằng Docker Compose với CeleryExecutor, Redis và PostgreSQL metadata DB.
- Phân biệt orchestration với reusable data logic; DAG được giữ mỏng.

### Mốc 2 — FastOrder Business Scope — COMPLETE

- Chốt FastOrder là marketplace B2C giả lập tại Việt Nam.
- Xác định stakeholders, KPI, happy path, exception path và order state machine.
- Chốt business-first thay vì tool-first.

### Mốc 3 — Initial Architecture & Data Model — COMPLETE

- Tạo High-Level, Logical, Physical Architecture và Data Flow Diagram.
- Olist được xác định là seed snapshot; PostgreSQL là operational source sống.
- Tạo data dictionary/business entities; một số phần được cập nhật dần theo implementation.

---

## Phase 2 — Operational PostgreSQL & Simulator (29/07–01/08/2026)

### Mốc 4 — Schema-First PostgreSQL — COMPLETE

- PostgreSQL 16 riêng cho FastOrder.
- `database/schema.sql` kiểm soát PK/FK/index/type.
- Geolocation dùng surrogate PK để bảo toàn duplicate source records.
- Initial Olist bootstrap chạy theo transaction và FK order.

### Mốc 5 — Post-Load Validation — COMPLETE

- Row-count reconciliation 9/9 tables.
- FK integrity PASS.
- Required NULL validation PASS.

### Mốc 6 — Warehouses & Sparse Inventory — COMPLETE

- Seed 5 warehouses: Hà Nội, Hải Phòng, Đà Nẵng, TP.HCM, Cần Thơ.
- Sparse inventory 2–3 warehouses/product, fixed seed 42.
- Idempotent seed và validation PASS.

### Mốc 7 — Faker Simulator — COMPLETE

- `CREATE_ORDER` transaction bao gồm orders/items/payments/inventory.
- Order state updater theo state machine, row locking và restock phù hợp.
- Bounded/continuous runner và independent validation hoàn tất.

---

## Phase 3 — Incremental Operational Ingestion (05–11/08/2026)

### Mốc 8 — Checkpoint, Extractor, Bronze Writer — COMPLETE

- Composite cursor dựa trên `updated_at + PK`.
- Atomic checkpoint.
- Pending Batch Context cho stable crash recovery.
- Deterministic extraction identity và replay-safe overwrite.

### Mốc 9 — Local Airflow E2E — COMPLETE

- Initial ingestion PASS.
- No-new-data rerun PASS.
- New source delta PASS.
- Failure-window/crash-recovery tests PASS.

### Mốc 10 — ADLS Bronze Integration — HISTORICAL / COMPLETE AT THAT TIME

- Operational writer từng được chuyển sang ADLS Gen2.
- Azure auth/storage probe và E2E crash-recovery validation hoàn tất.
- Mốc này sau đó bị supersede bởi local-first migration ngày 31/08/2026.

---

## Phase 4 — YOOCHOOSE File-Based Ingestion (13–15/08/2026)

### Mốc 11 — Source Preparation & Landing Design — COMPLETE LOGICALLY

- Profile clickstream ~33M events, 183 days.
- Chọn daily organization theo `event_date` thay vì arbitrary row chunks.
- Landing giữ 4 source fields; không business clean/deduplicate.

### Mốc 12 — File Ingestion Core — COMPLETE LOGICALLY

- File Discovery.
- Manifest Manager.
- Deterministic Bronze Writer.
- Runner với `NEW -> PENDING -> PROCESSED`, retry và skip semantics.
- Airflow DAG orchestration hoàn tất.

**Current note:** Azure-bound storage I/O của flow này đang chờ port sang MinIO.

---

## Phase 5 — Open-Meteo Weather Bronze & Silver (16–29/08/2026)

### Mốc 13 — Weather API Bronze — COMPLETE LOGICALLY

- Forecast và historical/backfill clients.
- Technical metadata contract.
- Deterministic UUID/ingestion identity.
- Sidecar commit unit: `response.json`, `metadata.json`, `_SUCCESS`.
- Forecast và historical Airflow flows đã được phát triển.

### Mốc 14 — Weather Forecast Silver — COMPLETE LOGICAL DESIGN / PORT PENDING

- Dataset `weather_forecast_hourly`.
- Grain: `warehouse_id + ingestion_id + forecast_time`.
- Incremental pending detection, transformation, DQ, append/persistence semantics và `NO_OP` đã được xác thực trên stack cũ.

### Mốc 15 — Weather Historical Silver — COMPLETE LOGICAL DESIGN / PORT PENDING

- Dataset `weather_history_hourly`.
- Grain: `warehouse_id + weather_time`.
- Overlap reconciliation.
- Separate `weather_history_processed_ingestions` control state.
- Delta MERGE + replay `NO_OP` đã được xác thực trên stack cũ.

---

## Phase 6 — Generic Operational Framework (30/08/2026)

### Mốc 16 — Config-Driven Generic Incremental Framework — COMPLETE

- `IncrementalTableConfig` cho schema/cursor/PK/type contract.
- Generic extractor hỗ trợ single/composite PK.
- Generic runner giữ nguyên pending/checkpoint crash-recovery protocol.
- DAG Factory tạo một DAG độc lập cho mỗi operational table.

### Mốc 17 — Operational Bronze on ADLS — COMPLETE HISTORICALLY

- 11 operational tables bootstrap + validation.
- Orders từng cần controlled re-bootstrap do legacy test artifacts.
- Bài học: Airflow green/checkpoint advancement không thay thế source↔Bronze reconciliation.

---

## Phase 7 — Local-First Migration (31/08/2026)

### Mốc 18 — MinIO Infrastructure — COMPLETE

- MinIO chạy trong Docker.
- Buckets: `landing`, `bronze`, `silver`, `gold`.
- `boto3` S3-compatible client được tích hợp vào Airflow worker.

### Mốc 19 — Operational Bronze Writer Port — COMPLETE

- Bronze writer dùng explicit PyArrow schema.
- Timestamp physical precision được khóa ở `timestamp[us]`.
- Deterministic MinIO object key và retry overwrite PASS.

### Mốc 20 — Orders MinIO Certification — COMPLETE

- 99,492 source rows = 99,492 distinct Bronze PK.
- 20 production Parquet files.
- Checkpoint/source upper match.
- Pending clean.
- Replay `NO_OP`.
- Không có `TIMESTAMP(NANOS)`.

### Mốc 21 — Operational PostgreSQL -> MinIO Bronze 11/11 — COMPLETE

- Single-PK tables PASS.
- Composite-PK tables PASS.
- `geolocation` >1M rows PASS.
- Full Operational Bronze E2E validation: **11/11 PASS**.

---

## Current Boundary

```text
Completed and certified:
PostgreSQL -> Airflow generic ingestion -> PyArrow -> MinIO Bronze

Next migration:
YOOCHOOSE storage I/O -> MinIO
Weather storage I/O -> MinIO
Then local Spark/Delta -> ClickHouse -> dbt -> Power BI
```
