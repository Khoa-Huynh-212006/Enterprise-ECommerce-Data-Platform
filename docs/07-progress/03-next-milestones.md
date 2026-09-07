File hiện tại vẫn coi YOOCHOOSE MinIO là milestone chưa hoàn thành. :contentReference[oaicite:5]{index=5}

Tôi sẽ **rewrite file này gọn lại hoàn toàn**, vì backlog cũ đã lệch phase.

```markdown
# Next Milestones

**Starting point:** 01/09/2026

Completed ingestion flows:

- Operational PostgreSQL → MinIO Bronze ✅
- YOOCHOOSE File → MinIO Landing/Bronze ✅

Current focus:

- Open-Meteo Weather API → MinIO Bronze

> Thứ tự dưới đây là dependency order, không phải deadline.

---
 ## M1 — Weather Bronze -> MinIO ✅ COMPLETE

### Kết quả

Weather API Bronze migration từ Azure Data Lake Storage sang MinIO đã hoàn tất.

### Forecast

- Bronze Writer -> MinIO PASS.
- Commit State (`_SUCCESS`) -> MinIO PASS.
- Single-warehouse Runner integration PASS.
- 5-warehouse aggregation PASS.
- Forecast Airflow DAG -> MinIO PASS.
- Production Open-Meteo Forecast:
  - total = 5
  - committed = 5
  - skipped = 0.
- Production Bronze reconciliation:
  - 5 ingestion units
  - 15 objects
  - PASS.

### Historical Forecast

- Deterministic historical ingestion identity PASS.
- Single Historical Runner -> MinIO PASS.
- Cross-run replay -> SKIP PASS.
- Historical batch orchestration:
  - 3 windows
  - 5 warehouses
  - 15 units
  - PASS.
- Historical Airflow DAG -> MinIO PASS.
- Production Historical backfill:
  - total = 15
  - committed = 15
  - skipped = 0.

### Definition of Done

- [x] Forecast 5 warehouses E2E PASS.
- [x] Historical/backfill E2E PASS.
- [x] Retry/replay không tạo duplicate logical ingestion.
- [x] `_SUCCESS` vẫn là commit boundary.
- [x] Weather production path không còn phụ thuộc ADLS storage client.

---

## M2 — Remove Azure Runtime Dependencies ✅ COMPLETE

- Azure storage client removed.
- ADLS Bronze writers removed.
- Azure-only Bronze tests removed or migrated.
- File Bronze Writer dedicated MinIO E2E PASS.
- Production Python/DAG runtime contains no Azure SDK imports.
- Airflow compile/import/DAG registration validation PASS.
- Operational, File and Weather ingestion layers now run local-first without Azure credentials.
---

## M3 — Local Spark + Delta Lake OSS

### Must-have

- Setup Local Spark/PySpark.
- Spark đọc MinIO bằng S3-compatible configuration.
- Delta Lake OSS hoạt động local.
- Test đọc production Operational và YOOCHOOSE Bronze.
- Verify Parquet compatibility.

### Definition of Done

- Spark read MinIO Bronze PASS.
- Delta create/read/update/merge smoke tests PASS.

---

## M4 — Port Weather Silver

Không redesign business semantics.

Giữ nguyên Forecast/Historical contracts đã chứng minh ở stack cũ.

### Definition of Done

- Forecast Silver E2E PASS.
- Historical Silver E2E PASS.
- Incremental pending detection PASS.
- Replay `NO_OP` PASS.

---

## M5 — Operational Silver

Ưu tiên:

- `orders_observed_versions`
- grain: `(order_id, updated_at)`

sau đó:

- `order_status_history`

Chỉ derive những state transition thực sự quan sát được.

Không fabricate lịch sử mà current-state polling chưa từng nhìn thấy.

---

## M6 — PostgreSQL DWH V1

- Add PostgreSQL DWH service riêng.
- Không dùng chung database với FastOrder OLTP.
- Thiết kế Silver → DWH loading boundary.
- Chọn Fact/Dimension dựa trên stakeholder KPI.
- Reconciliation Silver → DWH.

---

## M7 — dbt Core & Business Marts

- `dbt-postgres`.
- Sources.
- Staging models.
- Fact/Dimension.
- Business marts.
- dbt tests.
- Documentation.

---

## M8 — Power BI & V1 Completion

- Power BI dashboards.
- KPI validation.
- Runbook.
- Monitoring/DQ summary.
- README/architecture polish.
- Final E2E walkthrough.

---

# V2 Upgrade Backlog

Chỉ bắt đầu sau khi FastOrder V1 chạy E2E.

Các candidate:

- Manual YOOCHOOSE delivery → automated HTTP/SFTP/object-storage delivery.
- PostgreSQL DWH → ClickHouse.
- Basic batch observability → richer monitoring.
- Local state → stronger metadata/state infrastructure.
- Local runtime → deployment/cloud.
- Timestamp incremental extraction → CDC/streaming nếu có business need thực tế.

Mỗi upgrade phải trả lời:

1. V1 đang gặp vấn đề gì?
2. Tại sao vấn đề đó xảy ra?
3. Công nghệ/design mới giải quyết nó như thế nào?