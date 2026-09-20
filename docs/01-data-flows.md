# FastOrder Data Flows

## 1. Mục đích

Tài liệu này mô tả cách dữ liệu di chuyển xuyên suốt FastOrder Data Platform:

- dữ liệu phát sinh từ đâu;
- được ingest bằng cơ chế nào;
- được lưu ở layer nào;
- được transform như thế nào;
- được đưa vào Data Warehouse ra sao;
- cuối cùng được sử dụng cho analytics và Power BI như thế nào.

Tài liệu tập trung vào **data flow**.

Các quyết định về lựa chọn công nghệ được mô tả trong `03-technology-architecture.md` và lý do lựa chọn được lưu trong `05-decision-log.md`.

---

# 2. Các data domain chính

FastOrder hiện có ba domain dữ liệu chính:

```text
FastOrder Data Platform
│
├── Operational Commerce
│   ├── Customers
│   ├── Orders
│   ├── Order Items
│   ├── Products
│   ├── Sellers
│   ├── Payments
│   ├── Reviews
│   ├── Warehouses
│   ├── Inventory
│   ├── Geolocation
│   └── Product Category Translation
│
├── Weather
│   ├── Forecast
│   └── Historical Forecast
│
└── Clickstream
    └── YOOCHOOSE
```

Ba domain có phương thức ingestion khác nhau nhưng cùng hội tụ về Data Warehouse và analytical marts.

---

# 3. Operational Commerce Flow

## 3.1. Nguồn dữ liệu

Olist được sử dụng như dữ liệu seed để khởi tạo FastOrder Operational Database.

FastOrder không xem Olist CSV là analytical source cuối cùng.

Sau khi bootstrap:

```text
Olist CSV
    ↓
PostgreSQL Operational Database
    ↓
FastOrder OLTP
```

PostgreSQL trở thành source chính cho operational ingestion.

Operational database gồm 11 bảng:

```text
geolocation
customers
warehouses
orders
products
sellers
order_items
order_reviews
order_payments
product_category_name_translation
inventory
```

---

## 3.2. Operational ingestion

Luồng tổng quát:

```text
PostgreSQL OLTP
      ↓
Airflow
      ↓
Generic Incremental Ingestion
      ↓
MinIO Bronze
```

FastOrder sử dụng timestamp-based incremental extraction thay vì log-based CDC.

Watermark logic:

```text
(updated_at, primary_key...)
```

Ví dụ:

```text
orders:
(updated_at, order_id)

order_items:
(updated_at, order_id, order_item_id)
```

Composite cursor giúp xử lý đúng trường hợp nhiều record có cùng `updated_at`.

---

## 3.3. Upper watermark

Mỗi ingestion run chụp một upper watermark trước khi bắt đầu extraction.

Ví dụ:

```text
lower watermark
       ↓
source records
       ↓
upper watermark
```

Pipeline chỉ đọc dữ liệu:

```text
(lower_watermark, upper_watermark]
```

Điều này ngăn pipeline liên tục "đuổi theo" dữ liệu mới phát sinh trong lúc extraction đang chạy.

---

## 3.4. Checkpoint và Pending Recovery

FastOrder sử dụng hai loại control state chính:

```text
checkpoint
pending context
```

### Checkpoint

Checkpoint đại diện cho vị trí source đã được xử lý thành công.

### Pending context

Pending context lưu trạng thái của ingestion đang được xử lý nhưng chưa hoàn tất.

Cơ chế này hỗ trợ các tình huống:

```text
Source read
    ↓
Bronze write
    ↓
CRASH
```

hoặc:

```text
Bronze write
    ↓
Checkpoint update
    ↓
CRASH trước cleanup
```

Khi rerun, pipeline có thể xác định ingestion nào đã ghi dữ liệu và ingestion nào cần tiếp tục.

---

# 4. Bronze Layer

Operational Bronze được ghi dưới dạng Parquet.

Luồng:

```text
PostgreSQL records
      ↓
Python objects
      ↓
PyArrow
      ↓
Parquet
      ↓
MinIO Bronze
```

Bronze có các đặc điểm:

- giữ gần với source;
- có ingestion metadata;
- append-oriented;
- replay-safe;
- không chứa business transformation nặng;
- không tự ý sửa business values.

Bronze có nhiệm vụ đảm bảo ingestion fidelity trước khi dữ liệu được chuẩn hóa ở Silver.

---

# 5. Operational Silver Flow

Luồng:

```text
MinIO Bronze
      ↓
Apache Spark
      ↓
Transformation
      ↓
Delta Lake Silver
```

Silver chịu trách nhiệm:

- schema normalization;
- deduplication;
- type normalization;
- business data-quality validation;
- canonical representation;
- replay-safe processing.

Operational Silver được triển khai cho toàn bộ 11 operational tables.

---

# 6. Silver → Data Warehouse

Sau Silver:

```text
Delta Lake Silver
      ↓
Spark JDBC Loader
      ↓
PostgreSQL DWH
      ↓
staging schema
```

DWH loader sử dụng staging/load validation trước khi thay đổi target.

Mục tiêu là tránh trường hợp:

```text
target bị truncate
        ↓
load lỗi giữa chừng
        ↓
DWH mất dữ liệu
```

Load flow được thiết kế theo hướng:

```text
Silver
   ↓
temporary load table
   ↓
validation
   ↓
transaction
   ↓
staging target
```

---

# 7. dbt Transformation Flow

Sau staging:

```text
staging
    ↓
dbt sources
    ↓
dbt intermediate
    ↓
dbt marts
```

Ba schema chính:

```text
staging
intermediate
marts
```

Một số intermediate models quan trọng:

```text
int_order_payments_agg
int_order_items_agg
int_orders_enriched
int_clickstream_sessions
```

Ví dụ:

```text
orders
   +
order items aggregation
   +
payment aggregation
   ↓
int_orders_enriched
   ↓
fact_orders
```

dbt chịu trách nhiệm:

- analytical transformation;
- fact/dimension modeling;
- business tests;
- source tests;
- mart construction.

---

# 8. Operational Analytics Flow

Operational mart flow:

```text
PostgreSQL OLTP
      ↓
Bronze
      ↓
Silver
      ↓
DWH staging
      ↓
dbt intermediate
      ↓
dbt marts
      ↓
Power BI
```

Core commerce marts:

```text
dim_customers
dim_products
dim_sellers
dim_warehouses
dim_date

fact_orders
fact_order_items
```

---

# 9. Weather Forecast Flow

## 9.1. Source

Weather data được lấy từ Open-Meteo.

FastOrder sử dụng weather context cho 5 warehouses.

Forecast flow:

```text
Open-Meteo Forecast API
        ↓
Airflow
        ↓
Weather API Client
        ↓
MinIO Bronze
```

---

## 9.2. Bronze contract

Mỗi forecast ingestion unit gồm:

```text
response.json
metadata.json
_SUCCESS
```

Ý nghĩa:

### `response.json`

Raw response từ Open-Meteo.

Không inject FastOrder metadata trực tiếp vào payload.

### `metadata.json`

Chứa FastOrder technical metadata, ví dụ:

```text
ingestion_id
warehouse_id
requested_at
api_type
```

### `_SUCCESS`

Commit marker.

Một ingestion unit chỉ được xem là hoàn thành nếu `_SUCCESS` tồn tại.

---

# 10. Weather Forecast Silver

Flow:

```text
Bronze Forecast Snapshot
        ↓
Spark
        ↓
Flatten hourly arrays
        ↓
Attach ingestion metadata
        ↓
Data Quality
        ↓
Delta Silver
```

Forecast Silver grain:

```text
warehouse_id
+
ingestion_id
+
forecast_time
```

`ingestion_id` được giữ để phân biệt nhiều forecast snapshots cho cùng một thời điểm forecast.

---

# 11. Historical Weather Flow

Historical weather được sử dụng để bootstrap/backfill analytical context.

Flow:

```text
Open-Meteo Historical Forecast
        ↓
Historical ingestion
        ↓
Bronze
        ↓
Spark
        ↓
Silver
        ↓
DWH
        ↓
dbt
```

Historical Silver grain:

```text
warehouse_id
+
weather_time
```

Historical weather không nằm trong normal recurring top-level platform run.

Nó được xem là:

```text
bootstrap / backfill workflow
```

và được chạy độc lập khi cần.

---

# 12. Weather Analytics Marts

Weather marts:

```text
fact_weather_forecast_hourly
fact_weather_historical_forecast_hourly
```

Sau đó:

```text
Weather Facts
      ↓
Dim Date
Dim Warehouses
      ↓
Power BI
```

---

# 13. Clickstream Source Flow

FastOrder sử dụng YOOCHOOSE clickstream dataset.

Dataset chứa khoảng:

```text
33,003,944 click events
```

Source được profiling thành:

```text
183 event dates
```

Sau preparation, dữ liệu được tổ chức theo ngày:

```text
event_date=...
```

---

# 14. File-Based Clickstream Ingestion

Clickstream không sử dụng timestamp watermark giống PostgreSQL.

Thay vào đó, FastOrder sử dụng file-based ingestion.

Flow:

```text
Prepared YOOCHOOSE Files
        ↓
File Discovery
        ↓
Manifest Manager
        ↓
Bronze Writer
        ↓
MinIO Bronze
```

Manifest theo dõi trạng thái file.

Các trạng thái chính:

```text
NEW
PENDING
PROCESSED
RETRY
```

File identity được thiết kế deterministic để tránh ingest cùng một source file nhiều lần.

---

# 15. Clickstream Silver

Flow:

```text
Clickstream Bronze
        ↓
Spark
        ↓
Parse schema
        ↓
Validate events
        ↓
Delta Lake Silver
```

Silver path:

```text
s3a://silver/clickstream/yoochoose
```

Partition:

```text
event_date
```

---

# 16. Clickstream DWH

Silver clickstream được load vào:

```text
staging.yoochoose_clicks
```

Khoảng:

```text
33,003,944 events
```

được giữ ở staging/Silver level.

FastOrder không tạo thêm một atomic `fact_click_events` chỉ để sao chép nguyên 33 triệu rows sang mart.

---

# 17. Clickstream Sessionization

dbt thực hiện session-level modeling.

Flow:

```text
staging.yoochoose_clicks
        ↓
int_clickstream_sessions
        ↓
fact_clickstream_sessions
        ↓
agg_clickstream_daily
```

`int_clickstream_sessions` có grain:

```text
1 row / session
```

Khoảng:

```text
9.25 million sessions
```

được tạo từ khoảng 33 triệu events.

---

# 18. Clickstream Domain Boundary

YOOCHOOSE `item_id` và Olist/FastOrder `product_id` không có shared business key.

Do đó FastOrder không tạo relationship:

```text
Clickstream → Products
Clickstream → Orders
Clickstream → Customers
```

chỉ để có conversion dashboard.

Điều này đồng nghĩa các metric như:

```text
click → purchase conversion rate
session → FastOrder order conversion
```

không được khẳng định từ hai dataset hiện tại.

---

# 19. Top-Level Platform Orchestration

Sau khi từng domain được chứng nhận độc lập, FastOrder sử dụng master DAG:

```text
fastorder_platform_e2e
```

Flow:

```text
Operational E2E ───────────┐
                           │
Weather Forecast E2E ──────┼──► Final Platform Validation
                           │
Clickstream E2E ───────────┘
```

Historical weather được giữ ngoài normal master flow.

---

# 20. Operational E2E

Operational E2E gồm:

```text
PostgreSQL OLTP
      ↓
Bronze
      ↓
Silver
      ↓
DWH staging
      ↓
dbt sources
      ↓
dbt intermediate
      ↓
dbt marts
      ↓
dbt tests
```

TaskGroup được sử dụng để nhóm tasks theo operational table trong Airflow UI.

---

# 21. Final Platform Validation

Sau khi các domain hoàn thành:

```text
dbt test
```

được chạy như final platform validation.

Mục tiêu:

- phát hiện cross-domain model issue;
- xác nhận marts còn hợp lệ sau toàn bộ platform run;
- không đánh dấu master DAG thành công nếu analytics layer không vượt qua tests.

---

# 22. Power BI Consumption Flow

Power BI kết nối trực tiếp tới:

```text
PostgreSQL DWH
```

và sử dụng:

```text
marts schema
```

thay vì đọc trực tiếp từ:

```text
Bronze
Silver
Operational PostgreSQL
```

Flow:

```text
dbt marts
    ↓
PostgreSQL DWH
    ↓
Power BI Semantic Model
    ↓
Executive Overview
```

---

# 23. Data Flow Invariants

Các nguyên tắc xuyên suốt platform:

### Operational

```text
Source primary key phải được bảo toàn.
```

### Incremental ingestion

```text
Rerun không được tạo duplicate business records.
```

### Bronze

```text
Không thực hiện business cleaning phá vỡ source fidelity.
```

### Silver

```text
Business grain phải rõ ràng.
Duplicate grain phải được phát hiện.
```

### DWH

```text
Load phải được validate trước khi target được xem là hợp lệ.
```

### dbt

```text
Models phải PASS tests trước certification.
```

### Clickstream

```text
Không tạo cross-domain join nếu không có shared business key.
```

### Weather

```text
Forecast và historical có grain khác nhau.
```

### BI

```text
Measure phải được dùng đúng grain và đúng Fact.
```

---

# 24. Current Data Flow Summary

```text
                         ┌───────────────────────┐
                         │ PostgreSQL FastOrder  │
                         │        OLTP           │
                         └──────────┬────────────┘
                                    │
                                    ▼
                             Airflow Ingestion
                                    │
                                    ▼
                               MinIO Bronze
                                    │
                                    ▼
                            Spark + Delta Silver
                                    │
                                    ▼
                           PostgreSQL DWH Staging
                                    │
                                    ▼
                                  dbt
                                    │
                                    ▼
                            Analytical Marts
                                    │
                                    ▼
                                Power BI


YOOCHOOSE ──► File Ingestion ──► Bronze ──► Silver ──► DWH ──► dbt

Open-Meteo ─► API Ingestion ───► Bronze ──► Silver ──► DWH ──► dbt
```

FastOrder vì vậy không phải một ETL script đơn lẻ mà là một local-first multi-domain data platform với independent ingestion patterns hội tụ vào một analytical warehouse chung.