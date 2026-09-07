# Kiến trúc Logic (Logical Architecture)

## 1. Mục tiêu

Logical Architecture mô tả **hệ thống phải làm gì** và ranh giới trách nhiệm của từng vùng dữ liệu mà không gắn với Azure, MinIO, Spark hay bất kỳ vendor cụ thể nào.

---

## 2. Logical Zones

### 2.1. Source Systems

**Trách nhiệm:** sinh và lưu dữ liệu gốc của doanh nghiệp hoặc dữ liệu ngữ cảnh bên ngoài.

**Đặc điểm:** có thể là relational current-state, HTTP API hoặc file delivery; dữ liệu có thể thay đổi theo thời gian.

### 2.2. Landing Zone

**Trách nhiệm:** làm source boundary cho file-based ingestion sau khi file đã được tiếp nhận/prepared nhưng trước khi FastOrder xác nhận ingestion thành công.

**Lưu ý:** Landing không đồng nghĩa Bronze. Operational DB và API không bắt buộc phải đi qua Landing.

### 2.3. Data Ingestion

**Trách nhiệm:** vận chuyển dữ liệu từ source boundary vào Bronze và quản lý technical ingestion state.

Bao gồm:

- incremental extraction;
- file discovery và manifest;
- API request/retry;
- deterministic ingestion identity;
- checkpoint/pending state;
- technical contract validation.

Không bao gồm business cleansing hoặc business modeling.

### 2.4. Bronze / Raw Zone

**Trách nhiệm:** lưu dữ liệu mà FastOrder ingestion đã chấp nhận, bảo toàn source fidelity và lineage.

**Nguyên tắc:**

- Không fill/drop/deduplicate business data chỉ để pipeline PASS.
- Cùng một logical ingestion identity có thể được **overwrite an toàn khi retry**; điều này không tạo ra một logical version mới.
- Dữ liệu đã commit được xem là source of truth cho downstream replay.

### 2.5. Data Processing

**Trách nhiệm:** thực hiện transformation giữa các zone.

Ví dụ:

- parse và flatten;
- schema enforcement;
- time normalization;
- reconciliation;
- data quality;
- joining và business-compatible standardization.

### 2.6. Silver / Curated Zone

**Trách nhiệm:** lưu dữ liệu đã được chuẩn hóa và kiểm tra chất lượng ở grain rõ ràng.

Silver phải có:

- schema ổn định;
- business grain rõ;
- lineage đủ để truy ngược Bronze;
- incremental/replay semantics rõ;
- không che giấu data-quality violation.

### 2.7. Analytics Warehouse

**Trách nhiệm:** cung cấp storage/compute tối ưu cho OLAP, dimensional modeling và truy vấn phân tích.

Đây là nơi thích hợp để tổ chức staging, Fact/Dimension và các bảng phục vụ analytics thay vì bắt Data Lake chịu toàn bộ workload BI.

### 2.8. Business Data Marts / Gold

**Trách nhiệm:** đóng gói dữ liệu theo nhu cầu stakeholder và KPI, ví dụ Sales, Logistics, Inventory hoặc Finance.

Gold là **business-ready contract**, không nhất thiết phải đồng nghĩa với một bucket/file-system layer cụ thể.

### 2.9. Data Consumption

Dashboard, BI report và ad-hoc analytics truy cập dữ liệu đã qua các data contracts phía trên.

### 2.10. Control & Observability Plane

Control state nằm ngoài business data grain, ví dụ:

- Airflow run/task state;
- checkpoint;
- pending context;
- manifest;
- processed-ingestion control dataset;
- logging/metrics.

Không trộn control state vào business table chỉ để suy luận pipeline đã xử lý tới đâu.

---

## 3. Luồng logic tổng quát

```text
PostgreSQL OLTP --------+
                        |
YOOCHOOSE files --------+----> Airflow --------------------+
                        |                                  |
Open-Meteo API ---------+                                  v
                                                   MinIO Data Lake
                                              landing / bronze / silver
                                                           |
                                                           v
                                                  Local Spark + Delta
                                                           |
                                                           v
                                                   PostgreSQL DWH
                                                  (separate service)
                                                           |
                                                        dbt Core
                                                           |
                                                           v
                                                   Power BI Desktop

Orchestration & Observability điều phối xuyên suốt nhưng không nằm trên data path như một storage zone.
```
---

## 4. Design Principles

1. **Single Responsibility:** mỗi zone có trách nhiệm riêng.
2. **Source Fidelity:** Bronze giữ sự trung thực với nguồn.
3. **Idempotency:** retry cùng logical unit không tạo duplicate vật lý ngoài ý muốn.
4. **Stable State:** checkpoint/pending/manifest phải có contract rõ và fail-fast khi hỏng.
5. **Decouple Storage and Compute:** storage path không được chi phối business logic.
6. **Explicit Grain:** Silver và Gold phải khai báo grain trước khi viết transformation.
7. **Validation before Promotion:** dữ liệu chỉ được promote sang layer tiếp theo sau khi qua các validation phù hợp.


## 5. Technology Mapping

| Logical capability | Công nghệ | Trạng thái | Vai trò |
|---|---|---|---|
| Operational Source | PostgreSQL 16 | ✅ Implemented | FastOrder OLTP current-state |
| External API | Open-Meteo | ✅ Source/client implemented | Weather context |
| File Source | YOOCHOOSE | ✅ V1 implemented | Clickstream external file source |
| Orchestration | Apache Airflow 3.3 + CeleryExecutor | ✅ Implemented | DAG, retry, scheduling, observability |
| Data Lake | MinIO (S3-compatible) | ✅ Operational + File implemented | Landing/Bronze/Silver object storage |
| Object Storage Client | boto3 | ✅ Implemented | S3-compatible I/O to MinIO |
| Bronze serialization | PyArrow + Parquet | ✅ Operational + File implemented | Bronze structured storage |
| Processing | Local Apache Spark / PySpark | ⏳ Planned | Bronze → Silver compute |
| Table format | Delta Lake OSS | ⏳ Planned | Transactional Silver datasets |
| Analytics Warehouse V1 | PostgreSQL | ⏳ Planned | Analytical DWH, separate from OLTP |
| SQL modeling | dbt Core + dbt-postgres | ⏳ Planned | Fact/Dimension, marts, tests |
| Consumption | Power BI Desktop | ⏳ Planned | Dashboard/reporting |
| Warehouse upgrade V2 | ClickHouse | 📦 Backlog | OLAP upgrade after V1 is complete |