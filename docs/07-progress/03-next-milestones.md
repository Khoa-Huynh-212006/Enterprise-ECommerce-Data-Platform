# Next Milestones

**Starting point:** 31/08/2026 — sau khi Operational PostgreSQL -> MinIO Bronze đạt 11/11 E2E PASS.

> Thứ tự dưới đây là dependency order, không phải deadline. Chỉ chuyển sang milestone kế tiếp khi Definition of Done của milestone hiện tại đạt.

---

## M1 — Documentation & Operational DAG Cleanup

### Mục tiêu

Đóng sạch milestone Operational MinIO trước khi đụng source flow khác.

### Must-have

- Đồng bộ architecture/status/timeline/decision docs.
- Rename `perational_incremental_ingestion_dags.py` thành `operational_incremental_ingestion_dags.py`.
- Dọn stale `__pycache__`/serialized DAG metadata nếu cần.
- Xóa `adls_bronze_writer.py` sau khi xác nhận không còn production import.

### Definition of Done

- Airflow chỉ register đúng generic operational DAGs mong đợi.
- Full Operational Bronze validator vẫn PASS 11/11.

---

## M2 — YOOCHOOSE File Flow -> MinIO

### Must-have

- Source boundary ở `landing` bucket.
- Port File Discovery ADLS -> S3/MinIO listing.
- Port Bronze Writer ADLS -> MinIO.
- Giữ nguyên Manifest và NEW/PENDING/PROCESSED semantics.
- Giữ deterministic destination/retry overwrite.

### Definition of Done

- Initial file ingestion PASS.
- Rerun -> SKIP đúng.
- PENDING recovery -> RETRY đúng.
- Bronze object/content reconciliation PASS.

---

## M3 — Weather Bronze -> MinIO

### Must-have

- Port Weather Bronze Writer.
- Port committed-ingestion discovery/state khỏi ADLS SDK.
- Giữ nguyên sidecar contract:
  `response.json + metadata.json + _SUCCESS`.
- Forecast và historical DAG chạy trên MinIO.

### Definition of Done

- Forecast 5 warehouses E2E PASS.
- Historical/backfill E2E PASS.
- Retry không tạo duplicate logical ingestion.
- `_SUCCESS` vẫn là commit boundary.

---

## M4 — Remove Azure Runtime Dependencies

Chỉ làm sau M2 + M3.

### Must-have

- Xóa `storage/adls_client.py` nếu không còn import.
- Xóa Azure SDK dependencies không còn dùng.
- Xóa Azure env variables khỏi local branch.
- Grep toàn repo để phân loại Azure references còn lại thành `historical docs` hoặc `bug`.

### Definition of Done

- Production code path không cần Azure credential/package.
- Operational/File/Weather Bronze đều chạy local-first.

---

## M5 — Local Spark + Delta Lake OSS

### Must-have

- Spark đọc/ghi MinIO qua S3-compatible configuration.
- Delta Lake OSS hoạt động local.
- Test đọc production Operational Parquet.
- Verify explicit `timestamp[us]` compatibility bằng Spark thực tế.

### Definition of Done

- Spark read MinIO Bronze PASS.
- Delta create/read/update/merge smoke tests PASS.

---

## M6 — Port Weather Silver

Không redesign business semantics.

### Forecast

- Grain: `warehouse_id + ingestion_id + forecast_time`.
- Committed Bronze discovery.
- Pending detection.
- DQ + validation.
- Persistence + replay `NO_OP`.

### Historical

- Grain: `warehouse_id + weather_time`.
- Overlap reconciliation.
- `weather_history_processed_ingestions` control dataset.
- Delta MERGE.
- Replay `NO_OP`.

### Definition of Done

- Kết quả logic tương đương implementation đã chứng minh trên stack cũ.

---

## M7 — Operational Silver

Bắt đầu sạch sau khi Spark/Delta ổn định.

Ưu tiên đầu tiên:

- `orders_observed_versions` — grain `(order_id, updated_at)`.
- `order_status_history` — chỉ derive transition đã quan sát, không fabricate unseen state.

Sau đó mở rộng sang các operational entities còn lại.

---

## M8 — ClickHouse Data Warehouse

- Add ClickHouse Docker service.
- Thiết kế staging/core loading boundary từ Silver.
- Chọn Fact/Dimension theo KPI/business needs.
- Reconciliation Silver -> DWH.

---

## M9 — dbt Core & Business Marts

- `dbt-clickhouse` project.
- Sources/staging models.
- Fact/Dimension models.
- Business marts.
- dbt tests và documentation.

---

## M10 — Power BI, Operations & Portfolio Polish

- Power BI dashboards theo stakeholder KPI.
- Runbook.
- Monitoring/data-quality summary.
- CI checks phù hợp.
- README/architecture polish.
- Final English translation sau khi Vietnamese docs ổn định.
