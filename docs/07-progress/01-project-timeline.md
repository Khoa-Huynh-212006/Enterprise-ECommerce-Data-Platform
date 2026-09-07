# Project Timeline — FastOrder Enterprise E-Commerce Data Platform

**As of:** 2026-09-01
**Current phase:** Weather Bronze → MinIO Migration

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

---

## Mốc 34 — YOOCHOOSE File Flow Migrated to MinIO

### 01/09/2026

**Mục tiêu**

Hoàn thiện File-Based Ingestion trên local-first stack và loại bỏ ADLS khỏi runtime YOOCHOOSE.

### Source Delivery & Validation

- Manual download `yoochoose-data.7z` được sử dụng làm external provider delivery cho V1.
- Extract `yoochoose-clicks.dat`.
- Raw file size: `1,486,798,186 bytes`.
- Validated `33,003,944` records.
- Malformed rows: `0`.
- Event dates: `183`.
- Timestamp range:
  `2014-04-01T03:00:00.124Z → 2014-09-30T02:59:59.430Z`.
- Phát hiện raw source không được sort theo `event_date`.

### Preparation

- Sử dụng local temporary workspace để partition raw records theo event date.
- Tạo đúng `183` daily headerless CSV files.
- Local temporary area không được xem là Landing zone.
- Upload prepared files vào canonical MinIO Landing.

### MinIO Landing

Target:

`landing/clickstream/yoochoose/prepared/event_date=YYYY-MM-DD/part-000.csv`

Validation:

- Uploaded: `183/183`.
- Total bytes: `1,486,798,186`.
- File Discovery: `183/183`.

### File Ingestion Migration

Port thành công:

- File Discovery: ADLS → MinIO.
- Bronze Writer: ADLS → MinIO.
- File Ingestion Runner: ADLS → MinIO.
- Airflow DAG: ADLS client → boto3 MinIO client.

Giữ nguyên:

- NEW/PENDING/PROCESSED manifest semantics.
- deterministic ingestion identity.
- logical `discovered_at`.
- retry-safe deterministic Bronze path.

### Recovery Validation

Integration tests PASS:

- First run processing.
- PROCESSED → SKIP.
- PENDING → RETRY.
- ingestion identity preservation.
- discovered timestamp preservation.
- post-Bronze crash recovery.
- no duplicate Bronze object.
- test artifact cleanup.

### Production Airflow E2E

DAG:

`yoochoose_file_ingestion`

Production result:

```text
discovered=183
processed=183
skipped=0
retried=0


## Mốc 35 — Weather API Bronze Migrated to MinIO

### 01/09/2026

**Mục tiêu**

- Hoàn tất migration luồng Open-Meteo Forecast và Historical Forecast từ ADLS sang MinIO.
- Giữ nguyên ingestion identity, commit semantics, retry/replay behavior và Bronze sidecar contract đã được chứng minh trước đó.

**Đã thực hiện**

### Weather Bronze Writer

- Thay Azure `FileSystemClient` bằng boto3 S3-compatible MinIO client.
- Giữ nguyên:
  - raw `response.json`;
  - technical `metadata.json`;
  - deterministic Bronze paths.
- Forecast Bronze Writer MinIO E2E PASS.
- Retry cùng ingestion identity ghi lại cùng object keys, không sinh duplicate.

### Weather Ingestion State

- Port `_SUCCESS` existence check sang `head_object`.
- Chỉ coi `404 / NoSuchKey / NotFound` là marker chưa tồn tại.
- Các infrastructure error khác vẫn fail-fast.
- `_SUCCESS` write sử dụng deterministic `put_object`.
- State MinIO E2E PASS.

### Forecast Flow

- Port Forecast Runner sang MinIO.
- Single-warehouse integration PASS:
  - first run COMMITTED;
  - replay SKIPPED;
  - replay dừng trước API call.
- Multi-warehouse Runner PASS cho đủ 5 warehouses.
- Port `weather_forecast_ingestion` Airflow DAG sang MinIO.
- Production Forecast run PASS:
  - total = 5
  - committed = 5
  - skipped = 0.
- Production Bronze reconciliation PASS:
  - 5 ingestion units;
  - 15 objects;
  - metadata lineage đúng;
  - `_SUCCESS` đúng commit marker.

### Historical Forecast Flow

- Giữ historical identity:
  `warehouse_id + start_date + end_date`.
- Historical identity test PASS.
- Port single Historical Runner integration sang MinIO.
- Cross-run replay PASS:
  - `run_id` khác;
  - cùng warehouse/window;
  - cùng ingestion ID;
  - `_SUCCESS` tồn tại;
  - API không gọi lại;
  - SKIPPED.
- Historical Batch Runner PASS:
  - 90-day bootstrap;
  - 30-day deterministic windows;
  - 3 windows × 5 warehouses = 15 ingestion units.
- Trong quá trình test phát hiện và sửa một wiring bug thực tế:
  `inio_client` -> `minio_client`.
- Port `weather_historical_backfill` Airflow DAG sang MinIO.
- Production Historical backfill PASS:
  - `2026-06-03 -> 2026-07-02`
  - `2026-07-03 -> 2026-08-01`
  - `2026-08-02 -> 2026-08-31`
  - total = 15
  - committed = 15
  - skipped = 0.
- MinIO Bronze xác nhận đúng 3 historical window prefixes.

**Kết quả**

Weather Bronze migration từ ADLS sang MinIO: **COMPLETE**.

Ba ingestion flows của FastOrder hiện đã có Bronze implementation local-first:

```text
Operational PostgreSQL
        ↓
    MinIO Bronze

YOOCHOOSE Files
        ↓
MinIO Landing → MinIO Bronze

Open-Meteo API
        ↓
    MinIO Bronze

    