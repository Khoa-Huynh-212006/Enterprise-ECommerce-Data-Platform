# Current Project Status

**As of:** 31/08/2026  
**Current phase:** Local-First Architecture Migration

---

## 1. Executive Summary

FastOrder đã chuyển hướng khỏi Azure-paid development stack sang local-first zero-cost stack. **Operational PostgreSQL -> MinIO Bronze đã hoàn tất và được chứng nhận 11/11 tables.** File-based và Weather flows đã có logic ingestion/Silver quan trọng từ giai đoạn trước nhưng physical storage/runtime vẫn cần port khỏi Azure.

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

**Status: 🟡 Logical flow complete, MinIO port pending**

Đã có:

- daily source organization;
- File Discovery;
- Manifest Manager;
- Bronze Writer design;
- deterministic ingestion identity;
- NEW/PENDING/PROCESSED recovery semantics;
- Airflow DAG.

Còn lại:

- thay Azure/ADLS I/O bằng MinIO Landing/Bronze;
- recertify file flow E2E trên MinIO;
- cleanup Azure-specific dependencies.

---

## 5. Weather API

**Status: 🟡 Logical flow complete, MinIO/local-Spark port pending**

Đã có:

- Open-Meteo forecast ingestion;
- historical/backfill ingestion;
- metadata contract;
- `response.json + metadata.json + _SUCCESS` commit semantics;
- Forecast Silver design;
- Historical Silver overlap reconciliation/control state.

Còn lại:

- Azure Bronze I/O -> MinIO;
- ABFSS/Databricks runtime -> local Spark + S3-compatible paths;
- Delta Lake OSS persistence verification.

---

## 6. Analytics Layer

**Status: ⚪ Planned**

Target:

```text
MinIO Silver
  -> ClickHouse
  -> dbt Core
  -> Business Marts
  -> Power BI Desktop
```

Operational Silver sẽ được khởi động lại sạch sau khi local Spark/Delta đã ổn định; không tiếp tục các notebook Operational Silver cũ.

---

## 7. Current Technical Debt / Cleanup

- Rename `dags/perational_incremental_ingestion_dags.py` -> `operational_incremental_ingestion_dags.py`.
- Dọn stale Airflow DAG metadata/`__pycache__` sau rename.
- Xóa Azure-specific modules **chỉ sau khi** File + Weather replacements đã PASS.
- `storage/adls_client.py` tạm thời vẫn cần cho các flow chưa migrate.
- Docs cũ có Azure references được giữ như history trong Timeline/Decision Log, không dùng làm current physical architecture.
