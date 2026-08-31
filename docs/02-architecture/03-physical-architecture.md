# Kiến trúc Vật lý (Physical Architecture)

## 1. Mục tiêu

Tài liệu này ánh xạ Logical Architecture sang **stack hiện tại/target của FastOrder** sau quyết định chuyển sang môi trường local-first, zero-cost ngày 31/08/2026.

Điểm quan trọng: bảng dưới đây phân biệt rõ **Implemented**, **Migration pending** và **Planned** để tài liệu không mô tả target architecture như thể đã hoàn thành.

---

## 2. Technology Mapping

| Logical capability | Công nghệ | Trạng thái | Vai trò |
|---|---|---|---|
| Operational Source | PostgreSQL 16 | ✅ Implemented | FastOrder OLTP current-state |
| External API | Open-Meteo | ✅ Source/client implemented | Weather context |
| File Source | YOOCHOOSE | ✅ Source/preparation logic exists | Clickstream |
| Orchestration | Apache Airflow 3.3 + CeleryExecutor | ✅ Implemented | DAG, retry, scheduling, observability |
| Data Lake | MinIO (S3-compatible) | 🟢 In migration | Landing/Bronze/Silver object storage |
| Object Storage Client | boto3 | ✅ Implemented | S3-compatible I/O to MinIO |
| Bronze serialization | PyArrow + Parquet | ✅ Operational implemented | Explicit schema, `timestamp[us]` |
| Processing | Local Apache Spark / PySpark | ⏳ Planned port | Bronze → Silver compute |
| Table format | Delta Lake OSS | ⏳ Planned port | Transactional Silver datasets |
| Analytics Warehouse | ClickHouse | ⏳ Planned | Column-oriented OLAP/DWH |
| SQL modeling | dbt Core + dbt-clickhouse | ⏳ Planned | Fact/Dim, marts, tests |
| Consumption | Power BI Desktop | ⏳ Planned | Dashboard/reporting |

### Current certification boundary

Tại thời điểm cập nhật tài liệu:

```text
PostgreSQL OLTP
    -> Airflow generic incremental framework
    -> PyArrow Parquet
    -> boto3
    -> MinIO Bronze
```

đã được chứng nhận E2E cho **11/11 operational tables**.

YOOCHOOSE và Weather vẫn còn phần code/storage contract gắn với Azure từ giai đoạn trước và đang chờ migrate sang MinIO.

---

## 3. Target Runtime Architecture

```text
PostgreSQL OLTP -----+
                     |
YOOCHOOSE files -----+----> Airflow --------------------+
                     |                                  |
Open-Meteo API ------+                                  v
                                                MinIO Data Lake
                                           landing / bronze / silver
                                                        |
                                                        v
                                               Local Spark + Delta
                                                        |
                                                        v
                                                   ClickHouse
                                                        |
                                                     dbt Core
                                                        |
                                                        v
                                                Power BI Desktop
```

### Ghi chú về bucket `gold`

MinIO hiện có các bucket `landing`, `bronze`, `silver`, `gold`. Bucket `gold` được giữ như một physical namespace dự phòng, nhưng **business Gold/Data Marts target hiện tại sẽ được xây chủ yếu trong ClickHouse bằng dbt**. Không nên đồng nhất khái niệm logical Gold với một bucket cụ thể.

---

## 4. Lý do chọn công nghệ

### 4.1. Apache Airflow

- Code-first orchestration bằng Python.
- Phù hợp với pipeline có checkpoint, pending context, API retry và file manifest.
- Chạy local bằng Docker, không phụ thuộc cloud vendor.
- DAG được giữ mỏng; reusable logic nằm trong `fastorder/`.

### 4.2. MinIO + boto3

- MinIO cung cấp S3-compatible object storage semantics thay cho ADLS trong môi trường local.
- Tách storage khỏi local filesystem đơn thuần, vẫn giữ được object key/prefix, bucket và remote-I/O contract.
- `boto3` được chọn thay vì SDK riêng của MinIO để code bám vào S3-compatible interface và giảm vendor coupling.
- Operational Bronze giữ nguyên deterministic object key contract khi retry.

### 4.3. PyArrow + Parquet

Operational Bronze dùng explicit Arrow schema thay vì Pandas inference để khóa chặt kiểu dữ liệu vật lý. Đặc biệt timestamp được ghi ở precision microseconds (`timestamp[us]`) nhằm tránh lỗi Parquet `TIMESTAMP(NANOS)` khi đọc bằng Spark.

### 4.4. Local Spark + Delta Lake OSS

Spark vẫn là processing engine phù hợp với clickstream lớn và transformation nhiều nguồn. Delta Lake được dùng cho Silver khi cần persistent transactional table semantics, merge và processing state ổn định.

Trạng thái: logic Weather Silver đã được phát triển ở giai đoạn Databricks/ADLS, nhưng runtime/path cần được port sang local Spark + MinIO.

### 4.5. ClickHouse

ClickHouse là target DWH vì:

- column-oriented OLAP engine;
- chạy local bằng Docker;
- phù hợp truy vấn aggregation trên fact data lớn;
- là server database thực sự, phù hợp hơn embedded-only engine cho portfolio enterprise-style;
- tích hợp với dbt qua `dbt-clickhouse`.

### 4.6. dbt Core

Sau khi dữ liệu đã được chuẩn hóa và nạp/expose vào ClickHouse, dbt chịu trách nhiệm business SQL models, Fact/Dimension, marts và tests. Spark không nên gánh toàn bộ business modeling nếu SQL là công cụ phù hợp hơn.

---

## 5. MinIO Bucket Strategy

```text
landing  -> file-source boundary trước FastOrder ingestion
bronze   -> dữ liệu ingestion đã chấp nhận
silver   -> curated/validated datasets
 gold    -> reserved physical namespace; không bắt buộc là nơi duy nhất chứa logical Gold
```

Operational PostgreSQL và Weather có thể đi thẳng vào Bronze; Landing chủ yếu phục vụ file-based source như YOOCHOOSE.

---

## 6. State Storage

Trong MVP local-first, các control state nhỏ vẫn nằm trên shared Airflow volume:

```text
/opt/airflow/state/checkpoints/
/opt/airflow/state/pending/
/opt/airflow/state/file_based/
```

Đây là giới hạn có chủ ý của môi trường development. Không giả lập distributed metadata store nếu chưa có nhu cầu thực tế.

---

## 7. Legacy Azure Architecture

ADLS Gen2, Azure Databricks, Azure Synapse, ADF và Managed Identity thuộc **giai đoạn lịch sử trước 31/08/2026**. Các quyết định đó vẫn được giữ trong Decision Log để bảo toàn lịch sử thiết kế, nhưng không còn là target runtime hiện tại.
