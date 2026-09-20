# FastOrder Project Timeline

## 1. Mục đích

Tài liệu này lưu lịch sử phát triển của FastOrder từ cuối tháng 7/2026 đến thời điểm project được chốt ở phiên bản v1.

Timeline chỉ ghi các milestone có ý nghĩa lớn.

Các experiment nhỏ và từng commit riêng lẻ không được đưa vào đây.

Nếu không có bằng chứng ngày chính xác, timeline sử dụng khoảng thời gian thay vì tự suy đoán ngày.

---

# 23/07/2026 — Airflow Foundation

## Goal

Chuẩn bị orchestration environment trước khi bắt đầu data platform.

## Implemented

Thiết lập:

```text
Apache Airflow 3.3
Docker Compose
CeleryExecutor
Redis
PostgreSQL metadata
```

Làm quen với:

```text
Scheduler
Worker
API Server
DAG Processor
Triggerer
```

## Key Learning

Airflow chỉ nên chịu trách nhiệm orchestration.

Reusable data logic không nên nằm toàn bộ trong DAG.

## Result

Local Airflow infrastructure sẵn sàng cho FastOrder.

---

# 24–27/07/2026 — Business Scope

## Goal

Tránh xây project dạng:

```text
CSV → Spark → Dashboard
```

## Implemented

Chọn business context:

```text
FastOrder
B2C E-Commerce Marketplace
```

Xác định:

- customer journey;
- order lifecycle;
- stakeholders;
- KPI groups;
- warehouses;
- sellers;
- inventory;
- payment;
- logistics.

Business design scale:

```text
~50K orders/day
~200 sellers
5 warehouses
```

## Result

Project có business narrative trước khi chọn final tools.

---

# 28/07/2026 — Source Systems and Initial Data Model

## Implemented

Chọn:

```text
Olist
```

làm seed source cho commerce.

External context:

```text
Open-Meteo
```

File-based behavioral source:

```text
YOOCHOOSE
```

Bắt đầu thiết kế:

- operational entities;
- data dictionary;
- source boundaries.

---

# 29/07/2026 — PostgreSQL OLTP and Schema-First Design

## Implemented

Tạo FastOrder PostgreSQL riêng.

Phân biệt:

```text
Airflow PostgreSQL
≠
FastOrder PostgreSQL
```

Tạo:

```text
database/schema.sql
```

Operational tables được đổi sang business-oriented names:

```text
customers
orders
products
sellers
...
```

Thêm:

```text
warehouses
inventory
created_at
updated_at
PK
FK
indexes
```

## Key Decisions

- schema-first;
- SQLAlchemy Core;
- no Pandas-managed production schema;
- surrogate key cho geolocation;
- thin DAG principle.

---

# 30/07–01/08/2026 — Operational Simulation

## Goal

Biến static seed thành operational system có biến động.

## Implemented

Phát triển simulator concepts cho:

- new orders;
- inventory changes;
- order status changes.

Thiết kế:

- bounded mode;
- continuous mode;
- fail-fast;
- transaction protection;
- inventory overselling prevention.

## Result

FastOrder PostgreSQL có thể được xem như operational source thay vì static imported dataset.

---

# 05/08/2026 — Incremental Ingestion Design

## Implemented

Chốt:

```text
timestamp-based incremental extraction
```

Watermark:

```text
(updated_at, primary_key...)
```

Thiết kế:

```text
lower watermark
upper watermark
batch extraction
checkpoint
pending recovery
```

## Decision

Không triển khai WAL-based CDC trong MVP.

---

# 07–10/08/2026 — Local Incremental Ingestion MVP

## Implemented

Operational ingestion chạy qua Airflow.

Đã test:

```text
initial ingestion
no-new-data rerun
new source delta
crash recovery
checkpoint recovery
```

## Result

Local Airflow Incremental Ingestion MVP PASS.

---

# 11/08/2026 — Azure ADLS Bronze Certification

## Goal

Thử nghiệm cloud object storage.

## Implemented

Operational Bronze được port lên:

```text
Azure Data Lake Storage Gen2
```

Đã test:

- idempotency;
- multi-batch;
- crash after Bronze;
- crash after checkpoint;
- partition date handling.

## Result

Operational ingestion được chứng minh chạy E2E tới ADLS Bronze.

---

# 13/08/2026 — YOOCHOOSE Cloud Preparation

## Source

YOOCHOOSE clickstream.

## Implemented

Sử dụng:

```text
Azure Data Factory
ADLS
Databricks
Managed Identity
Unity Catalog
```

để chuẩn bị source.

Validated:

```text
33,003,944 events
183 event dates
```

Prepared data được chia theo:

```text
event_date
```

## Result

Clickstream source preparation hoàn tất mà không mất rows.

---

# Mid August 2026 — File-Based Ingestion

## Goal

Xây ingestion pattern khác với database watermark.

## Implemented

Thiết kế:

```text
File Discovery
File Identity
Manifest Manager
Bronze Writer
```

Manifest statuses:

```text
NEW
PENDING
PROCESSED
RETRY
```

## Result

FastOrder có riêng một ingestion pattern cho file sources.

---

# 16/08/2026 — Weather API Architecture

## Implemented

Chọn Open-Meteo:

```text
Forecast API
Historical Forecast API
```

Forecast dùng cho ongoing ingestion.

Historical Forecast dùng cho bootstrap/backfill.

Bronze contract:

```text
response.json
metadata.json
_SUCCESS
```

## Result

Weather ingestion có source-preserving raw contract và deterministic identity.

---

# 22/08/2026 — Weather Silver Design

## Implemented

Weather Forecast Bronze được transform bằng PySpark.

Pipeline gồm:

```text
read Bronze
flatten hourly arrays
attach metadata
normalize timestamps
run Data Quality
write Silver
```

Historical weather cũng được thiết kế với overlap reconciliation.

---

# Late August 2026 — Reassessment of Cloud Architecture

## Problem

Azure-dependent development tạo:

- billing risk;
- account dependency;
- cloud resource lifecycle overhead;
- khó reproducible lâu dài.

## Decision

FastOrder sẽ chuyển sang local-first, zero-cost.

---

# 31/08/2026 — Local-First Migration

## Architecture Change

```text
ADLS
→ MinIO

Databricks
→ Local Spark

Cloud target DWH
→ Local analytical database
```

Operational ingestion được port sang MinIO.

## Result

11/11 operational tables được chứng nhận qua local Bronze ingestion.

---

# Early September 2026 — Spark + MinIO + Delta Lake

## Implemented

Local Spark được cấu hình để đọc/ghi MinIO bằng S3A.

Delta Lake OSS được đưa vào Silver layer.

Operational Silver được triển khai cho toàn bộ operational domain.

## Result

Local replacement cho Databricks/ADLS Silver architecture hoạt động.

---

# Early September 2026 — PostgreSQL DWH

## Architecture Decision

ClickHouse từng được xem xét.

Final choice:

```text
PostgreSQL 16
```

## Implemented

Tạo:

```text
staging
intermediate
marts
```

Silver → DWH loader chạy qua JDBC.

---

# Early September 2026 — dbt Analytics Layer

## Implemented

dbt sources cho:

```text
Operational
Weather
Clickstream
```

Operational intermediate:

```text
int_order_items_agg
int_order_payments_agg
int_orders_enriched
```

Core marts:

```text
dim_customers
dim_products
dim_sellers
dim_warehouses
dim_date

fact_orders
fact_order_items
```

## Result

Fact/Dimension model build và tests PASS.

---

# Early–Mid September 2026 — Weather E2E

## Forecast

Flow:

```text
Open-Meteo
→ Bronze
→ Silver
→ DWH
→ dbt
```

được orchestrate thành:

```text
fastorder_weather_forecast_e2e
```

## Historical

Historical bootstrap/backfill cũng được productionize thành independent E2E flow.

## Decision

Historical không nằm trong recurring master platform run.

---

# Early–Mid September 2026 — Clickstream E2E

## Data Size

```text
~33.0M click events
~9.25M sessions
183 days
```

## Implemented

```text
YOOCHOOSE
→ Bronze
→ Silver
→ DWH staging
→ dbt sessionization
→ fact_clickstream_sessions
→ agg_clickstream_daily
```

## Major Fix

DWH loader từng gặp memory pressure vì validate 33M rows bằng Spark.

Validation được chuyển phần lớn xuống PostgreSQL SQL aggregation.

Loader cũng hỗ trợ NO_OP nếu target đã khớp Silver.

## Result

Clickstream E2E PASS.

---

# Mid September 2026 — Operational Full E2E

## Implemented

Mỗi operational table được chạy qua:

```text
ingest
→ Bronze
→ Silver
→ DWH
```

Sau đó:

```text
dbt test
→ dbt build
```

## Result

Operational domain được certify end-to-end.

---

# Mid September 2026 — Master Platform Orchestration

## Implemented

Tạo:

```text
fastorder_platform_e2e
```

Flow:

```text
Operational E2E ───────────┐
Weather Forecast E2E ──────┼──► Final Validation
Clickstream E2E ───────────┘
```

Child DAG triggers được chuyển sang:

```text
deferrable=True
```

## Final Validation

Master DAG kết thúc bằng:

```text
dbt test
```

## Result

Full platform certification PASS.

---

# Mid September 2026 — Airflow TaskGroup Refactor

Operational E2E Graph được nhóm theo entity.

Ví dụ:

```text
orders
├── ingest
├── bronze_to_silver
└── silver_to_dwh
```

Mục tiêu:

- clean Graph UI;
- dễ debug;
- dễ quan sát domain flow.

---

# September 2026 — C Drive → D Drive Migration

## Problem

Docker, Spark, Delta và data platform tạo áp lực lớn lên ổ C.

## Implemented

Project root được chuyển sang:

```text
D:\Enterprise-ECommerce-Data-Platform
```

Docker Desktop storage cũng được chuyển sang ổ D.

## Validation

Platform được chạy lại sau migration.

## Result

Environment mới hoạt động bình thường.

---

# Mid–Late September 2026 — Power BI Semantic Model

Power BI kết nối:

```text
localhost:5434
fastorder_dwh
marts
```

Imported:

```text
5 dimensions
commerce facts
clickstream facts
weather facts
```

Relationships được thiết kế theo:

```text
Dimension 1 → * Fact
Single direction
```

Không tạo fact-to-fact relationship.

---

# 19–20/09/2026 — Executive Overview

## Implemented

Executive Overview gồm các phân tích:

```text
Revenue
Orders
Customers
AOV
Items Sold

Metric Trend
Customer State
Order Status
Product Categories
Warehouse Performance
Top Products
```

Bổ sung:

- DAX measures;
- metric parameter;
- dynamic titles;
- conditional formatting;
- highlight top bars;
- KPI trends.

---

# 19–20/09/2026 — Warehouse Simulation Enrichment

## Problem

Olist historical records hầu như không có warehouse assignment.

Trong FastOrder business model, orders phải thuộc warehouse.

## Decision

Assign warehouse deterministic.

Observed target distribution:

```text
WH_HCM ≈ 30%
WH_HN  ≈ 25%
WH_DN  ≈ 20%
WH_CT  ≈ 15%
WH_HP  ≈ 10%
```

Order items được đồng bộ warehouse theo parent order.

## Boundary

Warehouse assignment là FastOrder synthetic operational data, không phải Olist original truth.

---

# 20/09/2026 — BI Scope Freeze

## Decision

Power BI v1 dừng tại:

```text
Executive Overview
```

Các page còn lại chuyển thành future enhancements:

```text
Product & Seller Performance
Customer & Geography
Digital Behavior
Weather & Operations Context
```

## Reason

Không để visualization scope kéo dài Data Engineering project vô hạn.

---

# 20/09/2026 — Git Cleanup

Power BI `.pbix` lớn hơn GitHub normal file limit.

Decision:

```text
*.pbix
```

được loại khỏi normal Git tracking.

Local Power BI file vẫn được giữ trên development machine.

Generated/raw data directories cũng không được xem là source code artifacts cần version control.

---

# 20/09/2026 — Documentation Simplification

## Problem

Docs cũ bị phân mảnh thành:

```text
high-level architecture
logical architecture
physical architecture
data-flow diagram
current status
next milestones
learning log
...
```

Nhiều nội dung overlap và dễ stale.

## Decision

Docs được rút xuống 6 files:

```text
01-data-flows.md
02-business-analytics.md
03-technology-architecture.md
04-olap-schema.md
05-decision-log.md
06-project-timeline.md
```

## Result

Mỗi document có một source-of-truth responsibility rõ ràng.

---

# FastOrder v1 Summary

Đến cuối project v1, FastOrder đã xây dựng được:

```text
Operational PostgreSQL
        ↓
Incremental Airflow Ingestion
        ↓
MinIO Bronze
        ↓
Spark + Delta Silver
        ↓
PostgreSQL DWH
        ↓
dbt Analytical Models
        ↓
Power BI
```

Ngoài commerce domain còn có:

```text
Weather API Pipeline
Clickstream File Pipeline
```

Platform bao gồm:

- multiple ingestion patterns;
- replay-safe processing;
- Bronze/Silver architecture;
- Data Warehouse;
- dimensional marts;
- data quality tests;
- Airflow E2E orchestration;
- multi-domain final validation;
- Power BI consumption layer.

---

# Future Enhancements

Các hướng mở rộng sau FastOrder v1 có thể bao gồm:

```text
additional Power BI pages
streaming / Kafka
real CDC
cloud deployment
CI/CD
automated data observability
more realistic warehouse routing
inventory analytics
customer behavior integration with shared IDs
```

Những phần này không nằm trong Definition of Done của FastOrder v1.