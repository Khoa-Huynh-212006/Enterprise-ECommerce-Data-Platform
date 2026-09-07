```markdown
# Current Project Status

**As of:** 01/09/2026  
**Current phase:** Weather API Bronze → MinIO Migration

---

## 1. Executive Summary

FastOrder đang chạy trên local-first zero-cost development stack.

Hai ingestion flows đã hoàn thành trên MinIO:

1. Operational PostgreSQL → MinIO Bronze: **11/11 tables PASS**.
2. YOOCHOOSE File Flow → MinIO Landing/Bronze: **E2E PASS**.

Flow ingestion còn lại cần migrate khỏi Azure là Open-Meteo Weather API.

Sau khi cả ba ingestion flows hoàn tất, project sẽ chuyển sang Local Spark/Delta → Silver → PostgreSQL DWH → dbt Core → Power BI.
---

## 2. Platform Status

| Component | Status | Notes |
|---|---|---|
| Docker local runtime | ✅ | Core development runtime |
| Apache Airflow 3.3 | ✅ | CeleryExecutor stack |
| PostgreSQL FastOrder OLTP | ✅ | Source of operational current-state data |
| MinIO | ✅ | Data Lake object storage |
| boto3 MinIO client | ✅ | S3-compatible storage access |
| MinIO buckets | ✅ | `landing`, `bronze`, `silver`, `gold` |
| Local Spark / PySpark | ⏳ | Chưa setup/port |
| Delta Lake OSS | ⏳ | Chưa setup/port |
| ClickHouse | ⏳ | Target DWH, chưa setup |
| dbt Core | ⏳ | Target modeling layer |
| Power BI Desktop | ⏳ | Target consumption layer |

---

## 3. Operational PostgreSQL -> Bronze

**Status: ✅ CERTIFIED**

- Generic config-driven incremental framework: PASS.
- Single PK + composite PK cursor: PASS.
- Checkpoint + Pending crash recovery: PASS.
- PyArrow explicit schema: PASS.
- MinIO deterministic overwrite: PASS.
- Orders replay `NO_OP`: PASS.
- Orders physical Parquet `timestamp[us]`: PASS.
- Full Operational Bronze E2E: **11/11 tables PASS**.

Operational tables:

`geolocation`, `customers`, `warehouses`, `orders`, `products`, `sellers`, `order_items`, `order_reviews`, `order_payments`, `product_category_name_translation`, `inventory`.

---
## 4. File-Based / YOOCHOOSE

**Status: ✅ E2E COMPLETE ON MINIO**

### Source preparation

V1 sử dụng manual external-provider delivery:

```text
YOOCHOOSE provider
→ local raw archive
→ yoochoose-clicks.dat
→ validation
→ daily preparation
→ MinIO Landing
```
---

## 5. Weather API

**Status: 🟢 Bronze ingestion on MinIO COMPLETE**

### Forecast

Đã hoàn tất:

- Open-Meteo Forecast ingestion cho 5 warehouse:
  - `WH_HN`
  - `WH_HP`
  - `WH_DN`
  - `WH_HCM`
  - `WH_CT`
- Bronze Writer đã migrate từ ADLS sang MinIO bằng boto3 S3-compatible client.
- Giữ nguyên sidecar contract:
  - `response.json`
  - `metadata.json`
  - `_SUCCESS`
- `_SUCCESS` tiếp tục là commit boundary.
- Forecast ingestion ID vẫn deterministic theo:
  `api_type + warehouse_id + Airflow run_id`.
- Retry cùng logical Airflow run sử dụng lại cùng ingestion root và SKIP ingestion đã commit.
- Single-warehouse Runner integration PASS.
- Multi-warehouse Runner test PASS.
- Airflow Forecast DAG đã migrate sang MinIO.
- Production Forecast run:
  - total = 5
  - committed = 5
  - skipped = 0
- Production Bronze reconciliation PASS:
  - 5 ingestion units
  - 15 expected objects
  - metadata lineage đúng
  - `_SUCCESS = COMMITTED`

### Historical Forecast

Đã hoàn tất:

- Historical Forecast bootstrap/backfill sử dụng Open-Meteo Historical Forecast API.
- Historical ingestion ID deterministic theo:
  `warehouse_id + start_date + end_date`.
- `run_id` không thuộc Historical identity.
- Cùng warehouse + cùng historical window ở DAG run khác vẫn map về cùng ingestion ID.
- Cross-run replay test PASS:
  - run đầu COMMITTED;
  - run sau với `run_id` khác nhưng cùng window → SKIPPED;
  - API không bị gọi lại;
  - không tạo duplicate logical ingestion.
- Historical Batch Runner PASS:
  - 90 ngày
  - window 30 ngày
  - 3 windows
  - 5 warehouses/window
  - 15 ingestion units.
- Airflow Historical Backfill DAG đã migrate sang MinIO.
- Production Historical backfill PASS:
  - total = 15
  - committed = 15
  - skipped = 0.
- Production windows:
  - `2026-06-03 -> 2026-07-02`
  - `2026-07-03 -> 2026-08-01`
  - `2026-08-02 -> 2026-08-31`
- MinIO Bronze đã xác nhận tạo đúng 3 historical window prefixes.

### Weather Bronze contract

```text
weather/open_meteo/
├── forecast/
│   └── ingestion_date=YYYY-MM-DD/
│       └── warehouse_id=.../
│           └── ingestion_id=.../
│               ├── response.json
│               ├── metadata.json
│               └── _SUCCESS
│
└── historical_forecast/
    └── window_start=YYYY-MM-DD/
        └── window_end=YYYY-MM-DD/
            └── warehouse_id=.../
                └── ingestion_id=.../
                    ├── response.json
                    ├── metadata.json
                    └── _SUCCESS

```markdown
## 6. Analytics Layer

**Status: ⚪ Planned**

V1 target:

```text
MinIO Silver
  → PostgreSQL DWH
  → dbt Core
  → Business Marts
  → Power BI Desktop
```

---

## 7. Current Technical Debt / Cleanup

- Rename `dags/perational_incremental_ingestion_dags.py` -> `operational_incremental_ingestion_dags.py`.
- Dọn stale Airflow DAG metadata/`__pycache__` sau rename.
- Xóa Azure-specific modules **chỉ sau khi** File + Weather replacements đã PASS.
- `storage/adls_client.py` tạm thời vẫn cần cho các flow chưa migrate.
- Docs cũ có Azure references được giữ như history trong Timeline/Decision Log, không dùng làm current physical architecture.
