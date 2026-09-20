# FastOrder Technology Architecture

## 1. Mục đích

Tài liệu này mô tả current technology architecture của FastOrder.

Câu hỏi chính:

> Ở mỗi giai đoạn của data lifecycle, FastOrder sử dụng công nghệ nào và công nghệ đó chịu trách nhiệm gì?

Current architecture là local-first và zero-cost.

Các kiến trúc Azure trước đây được giữ như historical architecture, không phải current runtime.

---

# 2. Current End-to-End Architecture

```text
                 ┌─────────────────────┐
                 │ PostgreSQL FastOrder│
                 │        OLTP         │
                 └──────────┬──────────┘
                            │
                            ▼
                   Apache Airflow 3.3
                            │
                            ▼
                     MinIO Bronze
                            │
                            ▼
              Apache Spark 3.5.9 / PySpark
                            │
                            ▼
                   Delta Lake OSS
                       Silver
                            │
                            ▼
                  PostgreSQL 16 DWH
                            │
                            ▼
                       dbt Core
                            │
                            ▼
                     dbt Marts
                            │
                            ▼
                   Power BI Desktop
```

Ngoài Operational source:

```text
YOOCHOOSE Files ─────────────► Airflow / MinIO

Open-Meteo API ──────────────► Airflow / MinIO
```

---

# 3. Technology Mapping

| Layer | Technology | Responsibility |
|---|---|---|
| Operational Source | PostgreSQL 16 | FastOrder OLTP |
| File Source | YOOCHOOSE | Clickstream behavior |
| External API | Open-Meteo | Weather |
| Orchestration | Apache Airflow 3.3 | Scheduling, retries, dependencies |
| Executor | CeleryExecutor | Distributed Airflow task execution |
| Queue | Redis | Celery message broker |
| Airflow Metadata | PostgreSQL | Airflow state |
| Object Storage | MinIO | Landing, Bronze, Silver namespaces |
| Bronze Serialization | PyArrow + Parquet | Operational Bronze |
| Processing | Apache Spark / PySpark 3.5.9 | Transformation |
| Table Format | Delta Lake OSS | Transactional Silver |
| Data Warehouse | PostgreSQL 16 | Staging + analytical warehouse |
| Transformation | dbt Core | Intermediate, marts, tests |
| dbt Adapter | dbt-postgres | PostgreSQL integration |
| BI | Power BI Desktop | Semantic model and dashboards |
| Runtime | Docker Compose | Local reproducible infrastructure |

---

# 4. Local-First Principle

FastOrder ưu tiên:

```text
reproducible
zero-cost
local development
portable architecture
```

Không cố giả lập:

- multi-region HA;
- enterprise IAM;
- Kubernetes;
- managed Spark;
- distributed metadata services;

nếu chúng không cần thiết cho mục tiêu học tập và portfolio.

---

# 5. Docker Runtime

Các service chính:

```text
postgres-fastorder
postgres
postgres-dwh

redis

airflow-apiserver
airflow-dag-processor
airflow-scheduler
airflow-triggerer
airflow-worker

minio
minio-init

spark

dbt
```

---

# 6. PostgreSQL Roles

## 6.1. Operational PostgreSQL

Service:

```text
postgres-fastorder
```

Role:

```text
FastOrder OLTP
```

Chứa:

- orders;
- customers;
- products;
- sellers;
- inventory;
- warehouses;
- payments;
- reviews;
- order items;
- operational reference data.

---

## 6.2. Airflow PostgreSQL

Service:

```text
postgres
```

Role:

```text
Airflow metadata database
```

Không chứa FastOrder business data.

---

## 6.3. DWH PostgreSQL

Service:

```text
postgres-dwh
```

Role:

```text
FastOrder analytical warehouse
```

Schemas:

```text
staging
intermediate
marts
```

---

# 7. Airflow Architecture

FastOrder chạy Apache Airflow 3.3 với CeleryExecutor.

Components:

```text
API Server
Scheduler
DAG Processor
Triggerer
Worker
Redis
PostgreSQL metadata
```

---

# 8. Thin DAG Principle

DAG files chỉ nên chứa orchestration.

Reusable logic nằm trong:

```text
fastorder/
```

Ví dụ:

```text
fastorder/
├── ingestion/
├── transformation/
├── loading/
├── orchestration/
├── db/
└── storage/
```

Không đặt toàn bộ:

```text
API client
Spark transformation
SQL loading
business validation
```

trực tiếp trong DAG file.

---

# 9. Airflow TaskGroup

Operational E2E sử dụng TaskGroup theo từng entity.

Ví dụ:

```text
customers
├── ingest
├── bronze_to_silver
└── silver_to_dwh

orders
├── ingest
├── bronze_to_silver
└── silver_to_dwh
```

Sau đó:

```text
analytics
├── test_sources
└── build_models
```

TaskGroup chủ yếu cải thiện:

- observability;
- Graph UI;
- logical organization.

---

# 10. Deferrable Orchestration

Master DAG sử dụng deferrable DAG triggers.

Mục tiêu:

- tránh giữ worker slot trong lúc chờ child DAG;
- giảm resource waste;
- giữ orchestration rõ ràng.

Top-level flow:

```text
Operational
Weather Forecast
Clickstream
        ↓
Final Validation
```

---

# 11. Spark Execution Boundary

Spark jobs chạy trong Spark runtime riêng.

Airflow worker có Docker CLI và Docker socket để trigger workload trong service tương ứng.

Điều này giữ:

```text
Airflow = orchestrator
Spark = compute engine
```

thay vì chạy heavy Spark workload trực tiếp trong Airflow Python process.

---

# 12. Spark Pool

FastOrder sử dụng Airflow pool:

```text
spark_local
```

với:

```text
1 slot
```

Mục đích:

- tránh nhiều heavy Spark jobs chạy song song trên laptop;
- giảm OOM;
- giảm disk pressure;
- giữ runtime ổn định.

---

# 13. MinIO Data Lake

Buckets:

```text
landing
bronze
silver
gold
```

---

# 14. Landing

Landing chủ yếu dùng cho file-based source.

Ví dụ:

```text
YOOCHOOSE prepared files
```

Landing đại diện cho source boundary trước khi FastOrder chính thức ingest file.

---

# 15. Bronze

Bronze chứa dữ liệu đã được ingestion framework chấp nhận.

Operational Bronze:

```text
Parquet
```

Weather Bronze:

```text
response.json
metadata.json
_SUCCESS
```

Clickstream Bronze:

```text
file-based accepted source data
```

---

# 16. Silver

Silver sử dụng:

```text
Apache Spark
+
Delta Lake OSS
```

Silver chịu trách nhiệm:

- normalization;
- data quality;
- deduplication;
- canonical grain;
- replay-safe transformations.

---

# 17. Gold Namespace

MinIO có bucket:

```text
gold
```

nhưng FastOrder không bắt buộc phải materialize toàn bộ analytical Gold vào object storage.

Logical Gold hiện được thể hiện chủ yếu bằng:

```text
PostgreSQL DWH
+
dbt marts
```

Điều này tránh tạo thêm physical layer chỉ để phù hợp hình thức Medallion Architecture.

---

# 18. PostgreSQL DWH

Ban đầu project từng xem xét ClickHouse làm analytics warehouse.

Sau quá trình triển khai, final implementation sử dụng:

```text
PostgreSQL 16
```

cho DWH.

Lý do thực tế:

- đã có PostgreSQL expertise;
- JDBC integration đơn giản;
- dbt-postgres ổn định;
- phù hợp scale hiện tại;
- giảm infrastructure complexity;
- đủ cho portfolio workload.

---

# 19. dbt Architecture

Version hiện tại:

```text
dbt Core 1.12.x
dbt-postgres 1.11.x
```

dbt chịu trách nhiệm:

```text
staging sources
      ↓
intermediate SQL models
      ↓
facts / dimensions
      ↓
aggregates
      ↓
tests
```

Spark không được dùng để thay thế mọi SQL transformation.

---

# 20. Power BI

Power BI Desktop là consumption layer.

Connection:

```text
Power BI
   ↓
PostgreSQL DWH
   ↓
marts
```

Power BI không đọc trực tiếp:

```text
Bronze
Silver
OLTP
```

---

# 21. Current Power BI Scope

Semantic model đã có:

```text
Dimensions
Facts
Relationships
DAX Measures
Executive Overview
```

Dashboard v1 tập trung Executive Overview.

Các dashboard còn lại được giữ cho future enhancement.

---

# 22. Weather Architecture

```text
Open-Meteo
    ↓
Airflow
    ↓
MinIO Bronze
    ↓
Spark
    ↓
Delta Silver
    ↓
PostgreSQL DWH
    ↓
dbt
```

Forecast và historical sử dụng shared source concept nhưng có grain và lifecycle khác nhau.

---

# 23. Clickstream Architecture

```text
YOOCHOOSE
    ↓
File-Based Ingestion
    ↓
MinIO Bronze
    ↓
Spark
    ↓
Delta Silver
    ↓
PostgreSQL DWH
    ↓
dbt Sessionization
```

Không tạo atomic click mart chỉ để duplicate staging.

---

# 24. Historical Azure Architecture

Trong tháng 8/2026 FastOrder từng triển khai một phần platform trên Azure.

Các công nghệ đã được sử dụng/thử nghiệm:

```text
Azure Data Lake Storage Gen2
Azure Data Factory
Azure Databricks
Azure Managed Identity
Unity Catalog
External Location / Volume
```

Ví dụ:

```text
YOOCHOOSE external source
        ↓
Azure Data Factory
        ↓
ADLS Landing
        ↓
Databricks
        ↓
prepared clickstream
```

Operational Bronze cũng từng được chứng nhận trên ADLS.

---

# 25. Vì sao Azure architecture bị superseded?

Mục tiêu project là:

```text
zero-cost
reproducible
student-friendly
portfolio-friendly
```

Cloud resources tạo ra:

- credit-card dependency;
- billing risk;
- account/resource lifecycle complexity;
- khó tái tạo project về sau.

Do đó ngày 31/08/2026 project chuyển sang local-first architecture.

---

# 26. Azure → Local Mapping

```text
ADLS Gen2
    ↓
MinIO

Databricks
    ↓
Local Spark

Synapse / ClickHouse target exploration
    ↓
PostgreSQL DWH

Cloud orchestration dependencies
    ↓
Docker Compose local runtime
```

Các Azure decisions vẫn được giữ trong historical timeline và decision log.

---

# 27. Storage Location

Project hiện chạy từ:

```text
D:\Enterprise-ECommerce-Data-Platform
```

Docker Desktop storage cũng được chuyển sang ổ D để tránh áp lực dung lượng trên ổ C.

Sau migration, platform được chạy lại để xác nhận local environment mới hoạt động.

---

# 28. Final Architecture Principle

FastOrder phân chia responsibility rõ ràng:

```text
PostgreSQL
→ operational state

Airflow
→ orchestration

MinIO
→ data lake storage

Spark
→ distributed transformation

Delta Lake
→ Silver table format

PostgreSQL DWH
→ analytics warehouse

dbt
→ analytical modeling

Power BI
→ semantic + visualization
```

Không có một tool duy nhất chịu trách nhiệm cho toàn bộ pipeline.

---

# 29. Current Architecture Summary

```text
Sources
│
├── PostgreSQL OLTP
├── YOOCHOOSE
└── Open-Meteo
        │
        ▼
      Airflow
        │
        ▼
       MinIO
        │
        ▼
 Spark + Delta Lake
        │
        ▼
 PostgreSQL DWH
        │
        ▼
       dbt
        │
        ▼
      Marts
        │
        ▼
    Power BI
```

Đây là architecture được xem là current FastOrder architecture.

Azure architecture được giữ lại như historical engineering experience, không còn là target runtime.