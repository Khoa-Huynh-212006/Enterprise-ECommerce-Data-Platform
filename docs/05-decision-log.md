# FastOrder Consolidated Decision Log

## 1. Mục đích

Tài liệu này lưu các quyết định quan trọng làm thay đổi kiến trúc hoặc cách FastOrder xử lý dữ liệu.

Đây là bản consolidated decision log.

Các quyết định thử nghiệm nhỏ và các implementation detail không còn ảnh hưởng tới current architecture đã được loại bỏ.

Status:

```text
ACTIVE
SUPERSEDED
```

---

# D-001 — Business-First Platform Design

**Date:** 24–27/07/2026  
**Status:** ACTIVE

## Context

Project có nguy cơ trở thành một demo công nghệ theo kiểu:

```text
CSV → Spark → Dashboard
```

## Decision

Bắt đầu từ:

```text
FastOrder business context
→ stakeholders
→ analytical requirements
→ data requirements
→ architecture
→ technology
```

## Reason

Một Data Engineering portfolio cần giải thích được business value, không chỉ tool stack.

## Consequence

FastOrder được xây như một B2C marketplace giả lập thay vì một dataset project.

---

# D-002 — Olist Is Seed Data, PostgreSQL Is the Operational Source

**Date:** 24–29/07/2026  
**Status:** ACTIVE

## Decision

Olist CSV chỉ dùng để bootstrap initial operational state.

Sau bootstrap:

```text
PostgreSQL
```

được xem là source của FastOrder.

## Reason

Cần một nguồn operational có thể:

- INSERT;
- UPDATE;
- incremental extract;
- replay;
- simulate lifecycle changes.

---

# D-003 — Schema-First Operational Database

**Date:** 29/07/2026  
**Status:** ACTIVE

## Decision

Database schema được kiểm soát bằng:

```text
database/schema.sql
```

Không để Pandas tự tạo production tables bằng:

```python
to_sql(if_exists="replace")
```

## Reason

Cần kiểm soát:

- data types;
- PK;
- FK;
- indexes;
- constraints;
- naming.

---

# D-004 — SQLAlchemy Core Instead of ORM

**Date:** 29/07/2026  
**Status:** ACTIVE

## Decision

Dùng:

```text
SQLAlchemy Core
+
psycopg2
```

## Reason

Project cần:

- connection management;
- transaction;
- parameterized SQL;
- Engine reuse;

nhưng không cần object-relational domain model.

---

# D-005 — Thin Airflow DAGs

**Date:** 29/07/2026  
**Status:** ACTIVE

## Decision

Reusable logic nằm trong:

```text
fastorder/
```

DAG chỉ orchestration.

## Reason

Giúp:

- unit testing;
- code reuse;
- clean DAG files;
- separation of concerns.

---

# D-006 — Timestamp-Based Incremental Extraction

**Date:** 05/08/2026  
**Status:** ACTIVE

## Decision

Không triển khai WAL-based CDC trong MVP.

Sử dụng:

```text
updated_at
```

làm incremental field.

## Reason

Timestamp incremental phù hợp với scope project và giảm complexity so với:

```text
Debezium
Kafka Connect
WAL replication
```

## Boundary

Đây là:

```text
timestamp-based incremental extraction
```

không được gọi là log-based CDC.

---

# D-007 — Composite Watermark

**Date:** 05/08/2026  
**Status:** ACTIVE

## Decision

Watermark:

```text
(updated_at, primary_key...)
```

thay vì chỉ:

```text
updated_at
```

## Reason

Nhiều rows có thể có cùng timestamp.

Composite cursor đảm bảo stable ordering và không skip record.

---

# D-008 — At-Least-Once Bronze + Replay Safety

**Date:** 05/08/2026  
**Status:** ACTIVE

## Decision

Bronze không giả lập distributed exactly-once transaction.

Pipeline được thiết kế theo:

```text
at-least-once ingestion
+
replay-safe downstream logic
```

## Reason

Object storage và source database không chia sẻ một distributed transaction.

Checkpoint, pending recovery và deduplication giải quyết failure scenarios rõ ràng hơn.

---

# D-009 — Separate Ingestion Fidelity and Business Data Quality

**Date:** 30/07/2026  
**Status:** ACTIVE

## Decision

Bronze giữ source fidelity.

Business cleaning chủ yếu được thực hiện ở:

```text
Silver
```

## Reason

Không muốn ingestion layer:

- tự sửa NULL;
- drop business records;
- che mất source issue.

---

# D-010 — Weather Forecast + Historical Forecast Strategy

**Date:** 16/08/2026  
**Status:** ACTIVE

## Decision

Sử dụng:

```text
Forecast API
```

cho ongoing operational ingestion.

Sử dụng:

```text
Historical Forecast API
```

cho bootstrap/backfill.

Không đưa multi-decade ERA5 climate analysis vào MVP.

## Reason

FastOrder cần logistics context và analytical cold-start data, không phải climate research.

---

# D-011 — Weather Bronze Sidecar Pattern

**Date:** 16/08/2026  
**Status:** ACTIVE

## Decision

Weather Bronze:

```text
response.json
metadata.json
_SUCCESS
```

## Reason

Không inject FastOrder metadata vào provider payload.

Tách:

```text
source truth
```

và:

```text
technical lineage
```

---

# D-012 — Deterministic API Identity and Commit Marker

**Date:** 16/08/2026  
**Status:** ACTIVE

## Decision

Weather ingestion sử dụng deterministic ingestion identity.

`_SUCCESS` là commit marker cuối cùng.

## Reason

Retry cùng logical ingestion không được tạo ra nhiều destination paths.

---

# D-013 — Azure Cloud Architecture

**Date:** 11–22/08/2026  
**Status:** SUPERSEDED

## Decision

Một giai đoạn của FastOrder từng sử dụng:

```text
ADLS Gen2
Azure Data Factory
Databricks
Managed Identity
Unity Catalog
```

## Result

Operational Bronze và YOOCHOOSE preparation đã được chứng minh trên Azure.

## Superseded By

```text
D-014 — Local-First Zero-Cost Architecture
```

---

# D-014 — Local-First Zero-Cost Architecture

**Date:** 31/08/2026  
**Status:** ACTIVE

## Decision

Chuyển toàn bộ current architecture sang local-first.

Mapping:

```text
ADLS → MinIO
Databricks → Local Spark
Cloud-centric runtime → Docker Compose
```

## Reason

Project cần:

- zero-cost;
- reproducibility;
- không phụ thuộc credit card;
- dễ duy trì lâu dài.

---

# D-015 — MinIO as Data Lake

**Date:** 31/08/2026  
**Status:** ACTIVE

## Decision

MinIO trở thành S3-compatible object storage chính.

Buckets:

```text
landing
bronze
silver
gold
```

## Reason

Giữ được object-storage architecture nhưng chạy hoàn toàn local.

---

# D-016 — Spark + Delta Lake OSS for Silver

**Date:** Early September 2026  
**Status:** ACTIVE

## Decision

Sử dụng:

```text
Apache Spark
+
Delta Lake OSS
```

cho Silver.

## Reason

Giữ distributed transformation semantics và transactional table format mà không cần Databricks.

---

# D-017 — PostgreSQL as Final DWH

**Date:** Early September 2026  
**Status:** ACTIVE

## Context

ClickHouse từng được xem là target analytical database.

## Decision

Final DWH sử dụng:

```text
PostgreSQL 16
```

## Reason

- đủ cho current data scale;
- JDBC integration đơn giản;
- dbt-postgres ổn định;
- giảm số lượng công nghệ phải vận hành;
- tập trung vào pipeline quality thay vì thêm infrastructure.

## Superseded

```text
ClickHouse target
```

---

# D-018 — dbt Owns Analytical SQL Modeling

**Date:** Early September 2026  
**Status:** ACTIVE

## Decision

Spark chịu trách nhiệm:

```text
Bronze → Silver
```

dbt chịu trách nhiệm:

```text
DWH sources
→ intermediate
→ facts/dimensions
→ marts
→ tests
```

## Reason

Không dùng Spark để thực hiện mọi transformation nếu relational SQL phù hợp hơn.

---

# D-019 — No Fake YOOCHOOSE ↔ Olist Join

**Date:** September 2026  
**Status:** ACTIVE

## Decision

Không join:

```text
YOOCHOOSE item_id
```

với:

```text
Olist/FastOrder product_id
```

## Reason

Không tồn tại shared business key được chứng minh.

## Consequence

Không tạo fake:

```text
click-to-purchase conversion rate
```

---

# D-020 — No Atomic 33M Click Event Mart

**Date:** September 2026  
**Status:** ACTIVE

## Decision

Không tạo:

```text
fact_click_events
```

33 triệu rows chỉ để copy lại staging.

## Reason

Analytical requirement hiện tập trung ở:

```text
session grain
daily aggregate
```

## Result

Sử dụng:

```text
fact_clickstream_sessions
agg_clickstream_daily
```

---

# D-021 — Safe DWH Load Before Target Replacement

**Date:** September 2026  
**Status:** ACTIVE

## Decision

Silver → DWH loader sử dụng temporary load target và validation trước final transaction.

## Reason

Tránh:

```text
truncate target
→ load fail
→ target empty
```

---

# D-022 — Historical Weather Runs Separately

**Date:** September 2026  
**Status:** ACTIVE

## Decision

Historical weather là:

```text
bootstrap / backfill flow
```

không nằm trong recurring:

```text
fastorder_platform_e2e
```

## Reason

Historical backfill không phải operational workload lặp lại mỗi platform run.

---

# D-023 — Deferrable Master DAG Triggers

**Date:** Mid September 2026  
**Status:** ACTIVE

## Decision

Top-level orchestration sử dụng deferrable child DAG triggers.

## Reason

Không giữ worker slot chỉ để chờ child DAG hoàn thành.

---

# D-024 — TaskGroup per Operational Entity

**Date:** Mid September 2026  
**Status:** ACTIVE

## Decision

Operational E2E Graph sử dụng:

```text
TaskGroup
```

theo entity.

## Reason

Improve:

- Airflow Graph readability;
- task grouping;
- operational observability.

---

# D-025 — Final Cross-Domain dbt Validation

**Date:** Mid September 2026  
**Status:** ACTIVE

## Decision

Master platform DAG kết thúc bằng:

```text
dbt test
```

## Reason

Một child DAG PASS chưa đủ nếu final analytical model bị invalid.

---

# D-026 — Project and Docker Storage Move to D Drive

**Date:** September 2026  
**Status:** ACTIVE

## Decision

Project root:

```text
D:\Enterprise-ECommerce-Data-Platform
```

Docker Desktop storage cũng được chuyển sang D.

## Reason

Ổ C không còn đủ dung lượng an toàn cho:

- Docker layers;
- Delta data;
- MinIO;
- Spark temp;
- Clickstream processing.

---

# D-027 — Warehouse as FastOrder Simulation Attribute

**Date:** 19–20/09/2026  
**Status:** ACTIVE

## Context

Olist historical data gần như không có warehouse mapping.

Tuy nhiên FastOrder business model có 5 warehouses và Olist được xem như backend seed của FastOrder.

## Decision

Assign warehouse deterministic cho orders thiếu warehouse.

Approximate distribution:

```text
WH_HCM 30%
WH_HN  25%
WH_DN  20%
WH_CT  15%
WH_HP  10%
```

Order items được đồng bộ theo order warehouse.

## Reason

Warehouse là một thuộc tính operational bắt buộc trong FastOrder simulation.

## Boundary

Đây là synthetic FastOrder business data.

Không được mô tả là observed Olist attribute.

---

# D-028 — Power BI v1 Scope Freeze

**Date:** 20/09/2026  
**Status:** ACTIVE

## Decision

BI v1 hoàn thiện:

```text
Executive Overview
```

Các page còn lại được chuyển sang future enhancement.

## Reason

Project trọng tâm là Data Engineering Platform.

Không kéo dài project chỉ để hoàn thiện số lượng dashboard.

---

# D-029 — Simplified Documentation Architecture

**Date:** 20/09/2026  
**Status:** ACTIVE

## Decision

Thay bộ docs phân mảnh bằng 6 files:

```text
01-data-flows.md
02-business-analytics.md
03-technology-architecture.md
04-olap-schema.md
05-decision-log.md
06-project-timeline.md
```

## Reason

Docs cũ:

- quá nhiều file;
- nội dung overlap;
- dễ stale;
- khó xác định source of truth.

## Result

Mỗi file có một responsibility duy nhất.

---

# Decision Log Summary

Current architectural principles của FastOrder:

```text
Business-first
Schema-first
Thin DAGs
Incremental + replay-safe
Bronze preserves source
Silver owns data quality
Local-first / zero-cost
Spark for Silver
PostgreSQL for DWH
dbt for analytical models
No unsupported cross-domain joins
Explicit analytical grain
```