# Project Timeline — FastOrder Enterprise E-Commerce Data Platform

**As of:** 2026-08-13
**Current phase:** File-Based Ingestion

> Quy ước: Các mốc có ngày chính xác được ghi theo bằng chứng có sẵn. Các mốc không đủ bằng chứng ngày cụ thể được ghi theo khoảng thời gian thay vì tự suy đoán.

---

## Mốc 1 — Khởi động hướng học và nền tảng Airflow

### 23/07/2026

**Mục tiêu**

- Chuẩn bị môi trường orchestration local.
- Hiểu các thành phần của Airflow trước khi đưa vào project.

**Đã thực hiện**

- Thiết lập Airflow 3.3.0 bằng Docker Compose.
- Sử dụng CeleryExecutor, Redis và PostgreSQL metadata database.
- Làm quen với DAG, Scheduler, Worker, API Server, Dag Processor và Triggerer.
- Tắt example DAG.
- Xác nhận các container có thể chạy trong Docker Desktop trên Windows.

**Khó khăn**

- Môi trường Windows/WSL2 có vấn đề đăng ký Ubuntu.
- Phải phân biệt PostgreSQL metadata của Airflow với PostgreSQL nghiệp vụ FastOrder.
- Làm rõ Airflow chỉ orchestration, không nên chứa toàn bộ business logic.

**Kết quả**

- Local Airflow infrastructure sẵn sàng để dùng ở giai đoạn incremental ingestion sau này.

---

## Mốc 2 — Xác định business scope và định hướng platform

### 24–27/07/2026 — khoảng thời gian phục dựng

**Mục tiêu**

- Tránh xây project theo kiểu “CSV → Spark → Dashboard”.
- Thiết kế một data platform có operational source đang thay đổi.

**Đã thực hiện**

- Chọn doanh nghiệp giả lập: **FastOrder**, marketplace B2C tại Việt Nam.
- Quy mô thiết kế: khoảng 50.000 đơn/ngày, nhiều seller và nhiều warehouse.
- Xác định stakeholders: CEO, Sales, Logistics, Inventory, Marketing, Finance và Customer Service.
- Xây business context, stakeholder KPI matrix, happy path, exception paths và order state machine.
- Chốt nguyên tắc: business requirement đi trước tool selection.

**Quyết định chính**

- Olist không phải database cuối cùng của FastOrder.
- Olist chỉ là snapshot seed để khởi tạo operational database.
- Faker simulator sẽ tạo INSERT/UPDATE để database trở thành nguồn OLTP “sống”.

**Kết quả**

- Project có business narrative rõ, phù hợp để giải thích trong CV và phỏng vấn.

---

## Mốc 3 — Hoàn thiện kiến trúc nhiều tầng

### 24–27/07/2026 — khoảng thời gian phục dựng

**Đã thực hiện**

- High-level architecture.
- Logical architecture.
- Physical architecture.
- Data Flow Diagram.
- Chốt target flow:

```text
Source Systems
    → Airflow
    → ADLS Bronze
    → Spark
    → ADLS Silver
    → Synapse
    → dbt Gold
    → Power BI
```

**Lưu ý quan trọng**

- Đây là **target architecture**.
- Current implementation ngày 29/07/2026 mới hoàn tất operational PostgreSQL bootstrap.
- Không được mô tả các thành phần ADLS, Synapse, dbt hay Power BI như đã triển khai.

---

## Mốc 4 — Chốt source systems và data model ban đầu

### 28/07/2026

**Đã thực hiện**

- Operational source:
  - Olist CSV làm seed.
  - PostgreSQL làm FastOrder OLTP.
  - Faker simulator sẽ tạo biến động.
- External APIs:
  - Open-Meteo.
  - Nager.Date.
  - Frankfurter.
  - Mock logistics API.
- File-based sources:
  - Clickstream JSONL.
  - Reviews JSONL theo định hướng ban đầu.
  - Vendor catalog CSV.
- Viết Data Dictionary v2 và business entities.

**Khó khăn phát hiện sau đó**

- Data Dictionary v2 chứa nhiều cột kế hoạch chưa tồn tại trong schema thực tế.
- Một số entity và tên cột không còn đồng bộ với implementation.
- Reviews trong implementation hiện được nạp vào PostgreSQL từ Olist, trong khi tài liệu cũ còn mô tả thêm `reviews.jsonl`.

**Kết quả**

- Có nền tảng tài liệu dữ liệu, nhưng cần nâng lên v3 để khớp schema đã triển khai.

---

## Mốc 5 — Dựng FastOrder PostgreSQL riêng

### Trước và trong ngày 29/07/2026

**Đã thực hiện**

- Tạo PostgreSQL 16 riêng cho FastOrder trong Docker.
- Phân biệt:
  - Airflow metadata PostgreSQL: host port 5432.
  - FastOrder operational PostgreSQL: host port 5433.
- Tải các file Olist vào `data/raw/olist/`.
- Không download dataset khi pipeline runtime.

**Kết quả**

- FastOrder có operational database độc lập với Airflow metadata DB.

---

## Mốc 6 — Thiết kế schema-first

### 29/07/2026

**Mục tiêu**

- Không để Pandas tự tạo schema bằng `if_exists="replace"`.
- Database schema phải do project kiểm soát.

**Đã thực hiện**

- Tạo `database/schema.sql`.
- Đổi bảng từ `olist_*` thành tên nghiệp vụ:
  - `customers`, `orders`, `products`, `sellers`, ...
- Thêm:
  - `warehouses`.
  - `inventory`.
  - `created_at`.
  - `updated_at`.
  - PK, FK và indexes.
- Bỏ:
  - `is_simulated`.
  - `simulation_id`.
- Thiết kế inventory bằng composite PK `(warehouse_id, product_id)`.
- Thêm index trên các khóa ngoại và `updated_at`.

**Khó khăn**

- `warehouses` ban đầu được tạo sau `orders` dù `orders` tham chiếu nó.
- `inventory` ban đầu thiếu `updated_at` nhưng lại tạo index cho cột đó.
- Composite PK geolocation dựa trên ZIP/lat/lng không phù hợp dữ liệu thực tế.

**Điều chỉnh**

- Tạo `warehouses` trước `orders`.
- Thêm timestamps cho `inventory`.
- Chuyển geolocation sang surrogate key để bảo toàn duplicate source records.

---

## Mốc 7 — Tổ chức lại Python package và connection layer

### 29/07/2026

**Đã thực hiện**

- Phân biệt:
  - `database/`: SQL artifacts.
  - `fastorder/db/`: Python code thao tác database.
- Chuyển connection logic:
  - Từ `fastorder/ingestion/db.py`.
  - Sang `fastorder/db/connection.py`.
- Đổi từ raw psycopg2 connection sang SQLAlchemy Core Engine.
- Dùng:
  - `URL.create()`.
  - Environment validation.
  - Engine dùng chung.
  - `SELECT 1` smoke test.

**Bài học**

- SQLAlchemy không thay driver; SQLAlchemy sử dụng psycopg2 bên dưới.
- `create_engine()` tạo Engine theo lazy connection, chưa mở kết nối thật ngay lập tức.
- Airflow DAG nên mỏng; logic tái sử dụng nằm trong `fastorder/`.

---

## Mốc 8 — Tạo script khởi tạo database

### 29/07/2026

**Đã thực hiện**

- Tạo `fastorder/db/init_db.py`.
- Script:
  - Tìm `database/schema.sql`.
  - Đọc UTF-8.
  - Lấy Engine.
  - Thực thi schema trong transaction.
  - Commit khi thành công, rollback khi lỗi.
- Chạy bằng:

```bash
python -m fastorder.db.init_db
```

**Đặc điểm vận hành**

- Đây là development bootstrap/reset script.
- Vì `schema.sql` có `DROP TABLE`, chạy lại sẽ xóa dữ liệu hiện tại.
- Không được đặt script này trong DAG định kỳ.

---

## Mốc 9 — Initial Olist loader

### 29/07/2026

**Đã thực hiện**

- Refactor `load_olist.py` để:
  - Dùng SQLAlchemy Engine.
  - Nạp vào schema đã tạo sẵn.
  - Dùng `if_exists="append"`.
  - Nạp theo thứ tự tôn trọng FK.
  - Kiểm tra đủ file trước khi mở transaction.
  - Dùng một transaction cho toàn bộ bootstrap.
  - Ném lại exception để tiến trình thất bại rõ ràng.
- Load order:

```text
geolocation
product_category_name_translation
customers
products
sellers
orders
order_items
order_payments
order_reviews
```

**Khó khăn**

- Geolocation vi phạm integrity constraint.
- Nguyên nhân: composite PK dựa trên tọa độ không phù hợp duplicate/precision của dữ liệu nguồn.
- Không chọn cách âm thầm `drop_duplicates()` trong loader.
- Sửa schema bằng surrogate key để giữ nguyên dữ liệu raw.

**Kết quả**

- Nạp thành công dữ liệu Olist vào PostgreSQL FastOrder.
- Initial operational snapshot hoàn tất.

---

## Mốc 10 — Post-load validation

### 30/07/2026
**Đã thực hiện**
- Post-load row-count reconciliation hoàn tất.
- Khẳng định foreign-key integrity validation hoàn tất.
- Hoàn thành Required NULL validation (kiểm tra 17 trường bắt buộc không có NULL).
- Quyết định dừng validation ở tầng cấu trúc vật lý, không thực hiện timestamp consistency validation tại file này.

**Kết quả**
- Chốt mốc Post-load validation hoàn tất. Toàn bộ 9/9 bảng khớp dòng, 6/6 quan hệ không orphan, 17/17 cột bắt buộc tuân thủ constraint.
---

## Mốc 11 — Thiết kế mạng lưới và Seed dữ liệu Warehouse

### 30/07/2026
**Đã thực hiện**
- Thiết kế mạng lưới 5 kho hàng chiến lược bao phủ 3 miền tại Việt Nam: Hà Nội, Hải Phòng (NORTH); Đà Nẵng (CENTRAL); TP.HCM, Cần Thơ (SOUTH).
- Xây dựng kịch bản `seed_warehouses.py` ứng dụng cơ chế UPSERT (`ON CONFLICT DO UPDATE`) để đảm bảo tính Idempotent.
- Tích hợp kiểm đếm trạng thái (`SELECT COUNT(*)`) ngay bên trong Transaction (`engine.begin()`) để xác nhận tính toàn vẹn.

**Kết quả**
- Chạy thành công quá trình Warehouse Seed.
- 5 bản ghi kho hàng đã được nạp an toàn vào PostgreSQL. Hạ tầng kho bãi đã hoàn tất và sẵn sàng cho bước phân bổ tồn kho (Inventory).

**Đã thực hiện**
- Seed idempotent 5 warehouse FastOrder.
- Sinh sparse inventory bằng fixed random seed 42.
- Mỗi sản phẩm có mặt tại 2–3 warehouse.
- Inventory seed dùng `ON CONFLICT DO NOTHING`.
- Inventory validation hoàn tất và toàn bộ rule PASS.

## Mốc 12 — Faker Simulator Implementation (CREATE_ORDER)

### 30/07/2026
**Đã thực hiện**
- Thiết kế và code thành công script `order_generator.py` mô phỏng sự kiện `CREATE_ORDER`.
- Cấu trúc Generator gói gọn 4 thao tác (`orders`, `order_items`, `order_payments`, `inventory`) trong 1 Transaction duy nhất.
- Chốt logic sinh dữ liệu: Random số mặt hàng theo trọng số (1-50), tra cứu giá/freight từ dữ liệu lịch sử bằng `DISTINCT ON`, áp dụng giới hạn B2C (max 10 items/line).
- Giải quyết thành công rủi ro Overselling/Race Condition bằng kỹ thuật Optimistic Concurrency Control (kiểm tra `rowcount` trên từng lệnh `UPDATE` tồn kho).

**Kết quả**
- Script tạo đơn hàng thành công, dữ liệu sinh ra sạch, chuẩn logic nghiệp vụ FastOrder VN và không vi phạm ràng buộc cơ sở dữ liệu.

### 31/07/2026:
- Hoàn thiện module `order_status_updater` với cơ chế khóa dòng (FOR UPDATE SKIP LOCKED) và hoàn trả tồn kho (Atomic Restock).
- Xây dựng thành công bộ kiểm thử chất lượng dữ liệu độc lập `validate_simulator.py`.
- Vượt qua toàn bộ 12 rules kiểm tra Data Invariants (Completeness, Consistency, Boundaries, Temporal & State Logic).
- Smoke tests `ADVANCE_ORDER_STATUS` và `SIMULATOR_VALIDATION`: **PASS**.


## Mốc 13 — Hoàn thiện Simulator Runner (Bounded & Continuous)

### 01/08/2026

**Mục tiêu**

- Hoàn thiện simulator runner hỗ trợ bounded và continuous mode để tự động hóa luồng sinh dữ liệu.

**Đã thực hiện**

- Hỗ trợ chạy hữu hạn hoặc liên tục.
- Tạo 0–3 orders mỗi cycle.
- Gọi status updater mỗi cycle.
- Tổng hợp cycle/session metrics.
- Hỗ trợ graceful shutdown bằng Ctrl+C.
- Chạy independent validator sau khi kết thúc.
- Smoke test continuous mode thành công.
- 12/12 validation rules PASS trên 15 simulated orders.

**Kết quả**

- Simulator operational workflow COMPLETE.

## Mốc 14 — Thiết kế và triển khai Checkpoint Manager

### 05/08/2026

**Mục tiêu**

- Implement local checkpoint management for timestamp-based incremental ingestion.

**Đã thực hiện**

- Initial checkpoint construction.
- Checkpoint contract validation.
- Safe handling of missing and corrupted checkpoint files.
- Atomic checkpoint replacement using temporary file, fsync and os.replace.
- Six checkpoint smoke-test scenarios completed.

**Kết quả**

- Checkpoint Manager: COMPLETE.

---

## Mốc 15 — Thiết kế và triển khai Orders Incremental Extractor

### 06/08/2026

**Mục tiêu**

- Design and implement Orders Incremental Extractor with bounded lower and upper composite watermarks.

**Đã thực hiện**

- Explicit schema contract via `SELECT` column list thay vì `SELECT *`.
- Python-side timestamp parsing và validation với `TIMESTAMP_FORMAT`.
- Upper watermark freezing để đóng băng biên trích xuất (prevent moving-target).
- Composite watermark bounds comparison bảo vệ logic `lower_position > upper_position`.
- Pagination handling sử dụng composite ties (`updated_at`, `order_id`).
- Smoke test với Data Quality Assertions (disjoint batches, monotonically increasing).

**Kết quả**

- Orders Incremental Extractor: Prototype COMPLETE.

## Mốc 16 — Thiết kế và triển khai Bronze Parquet Writer

### 06/08/2026

**Mục tiêu**

- Build an isolated module to serialize extraction records to Parquet with safe atomic replacement and strict metadata injection.

**Đã thực hiện**

- Input validation rules (empty batch prevention, datetime assertions).
- `updated_at` NULL checking to secure the composite watermark integrity.
- Injection of lineage metadata (`_ingestion_id`, `_ingested_at`, `_source_table`, `_source_updated_at`, `_ingestion_method`).
- Idempotent partition path generation (`ingestion_date`, `extraction_id`).
- PyArrow engine integration for optimized Parquet serialization.
- Corrected atomic file replacement suffix logic (`.parquet.tmp`).
- Comprehensive smoke testing with Pandas dataframe assertions (Row count, Disjoint checking, Metadata fidelity).

**Kết quả**

- Bronze Writer: Prototype COMPLETE.

## Mốc 17 — Incremental Runner & Crash Recovery

### 07/08/2026

**Mục tiêu**
- Ghép nối Extractor, Bronze Writer và Checkpoint Manager thành một Orchestrator hoàn chỉnh.
- Xây dựng cơ chế xử lý lỗi và tự phục hồi (Crash Recovery) bảo toàn dữ liệu.

**Đã thực hiện**
- Thiết kế Single-Batch và Multi-Batch loops với Progress Guards.
- Triển khai `pending_batch_manager.py` với cấu trúc JSON validation chặt chẽ và atomic writes.
- Áp dụng Commit Ordering: Extract -> Pending -> Write -> Checkpoint -> Delete Pending.
- Viết Smoke Tests toàn diện kiểm chứng Happy Path, Watermark fidelity, và các kịch bản Crash (trước/sau Checkpoint).
- Phân tách logic Next Batch Number và Batches Committed, đảm bảo không ghi đè file sau khi resume.

**Kết quả**
- Incremental Runner: COMPLETE. Khả năng tự phục hồi đạt chuẩn Production.

## Mốc 17 — Incremental Runner & End-to-End Crash Recovery

### 2026-08-07

**Mục tiêu**
- Hoàn thiện End-to-End Operational Validation cho Local Incremental Ingestion MVP.
- Chứng minh khả năng Crash Recovery và Commit Ordering với dữ liệu thật.

**Đã thực hiện**
- Tinh chỉnh logic Load Pending Context, ưu tiên Pending thay vì chụp Upper Watermark mới để đảm bảo tính nhất quán của Run đang dở.
- Tách biệt `batches_committed` và `next_batch_number` để ngăn chặn ghi đè ID khi resume.
- Bổ sung validation nghiêm ngặt cho `extraction_id` khi phục hồi.
- Chạy thành công toàn bộ E2E suite: Initial Load, No New Data, Simulator updates, và 2 kịch bản Crash/Recovery khắc nghiệt.

**Kết quả**
- Local Incremental Ingestion MVP: COMPLETE.

## Mốc 18 — Local Airflow Incremental Ingestion MVP

### 2026-08-10

**Mục tiêu**
- Đưa Incremental Runner lên Airflow DAG và chứng minh tính đúng đắn của toàn bộ chuỗi Pipeline End-to-End trên môi trường containerized.

**Đã thực hiện**
- Viết `orders_incremental_ingestion_dag.py` với cấu hình schedule manual và `max_active_runs=1`.
- Thiết lập cơ chế Sanitize Airflow Run ID đảm bảo an toàn cho File System.
- Áp dụng Fail-Fast validation cho các thư mục mount của Docker.
- Hoàn tất kiểm thử thực tế các kịch bản: Initial Ingestion, No-New-Data Rerun, và trích xuất Dữ liệu Delta.

**Kết quả**
- Local Airflow Incremental Ingestion MVP: COMPLETE. Hệ thống Ingestion đã chứng minh được sự trơn tru với dữ liệu thật.

## Mốc 19 — End-to-End Operational Validation (Local Airflow MVP Complete)

### 2026-08-10

**Mục tiêu**
- Chứng minh tính đúng đắn của toàn bộ chuỗi Pipeline Ingestion trên môi trường Airflow với dữ liệu thay đổi thực tế từ PostgreSQL.

**Đã thực hiện**
- Chạy thành công kịch bản Initial Ingestion (Vét cạn dữ liệu lịch sử).
- Chạy thành công kịch bản No-new-data rerun (Bảo vệ Data Lake khỏi file rác).
- Chạy thành công kịch bản New source delta (Bắt chính xác dữ liệu mới sinh ra từ Simulator).
- Nghiệm thu tính năng Crash Recovery và Composite Checkpoint trong môi trường vận hành thực tế.

**Kết quả**
- Local Airflow Incremental Ingestion MVP: COMPLETE. 
- Chuẩn bị bước sang Phase: ADLS Bronze Integration.

## Mốc 20 — ADLS Gen2 Bronze Integration & Full E2E Validation

### 2026-08-11

**Mục tiêu**
- Thay thế triệt để Local Bronze Storage bằng Azure Data Lake Storage Gen2.
- Kiểm định năng lực phục hồi hệ thống (Crash Recovery) trên hạ tầng Cloud.

**Đã thực hiện**
- Thiết kế `adls_bronze_writer.py` khai thác tối đa sức mạnh ghi in-memory (`io.BytesIO`) và tính năng overwrite của Azure SDK.
- Vượt qua chuỗi bài test Idempotency và Single/Multi-batch Integration trực tiếp trên mây.
- Giả lập thành công 2 case Crash Recovery chí mạng: Crash sau ADLS/trước Checkpoint và Crash sau Checkpoint/trước khi xóa Pending.
- Sửa lỗi lệch ngày Ingestion Partition bằng cách convert `Asia/Ho_Chi_Minh` timezone từ DAG.
- Hoàn tất bộ 4 bài test E2E thực tế trên Airflow UI.

**Kết quả**
- Milestone LOCAL AIRFLOW → ADLS GEN2 BRONZE INCREMENTAL INGESTION MVP: CHÍNH THỨC HOÀN TẤT.
- Đóng băng code cho module Ingestion Layer.

## 2026-08-13 — YOOCHOOSE Cloud Preparation Complete

### Objective

Prepare the historical YOOCHOOSE clickstream dataset entirely in Azure
for the FastOrder file-based ingestion flow.

### Completed

- Bootstrapped `yoochoose-data.7z` from the external provider to ADLS Landing using Azure Data Factory.
- Configured Databricks access to ADLS Landing using:
  - Access Connector
  - Managed Identity
  - Azure RBAC
  - Unity Catalog Storage Credential
  - External Location
  - External Volumes
- Extracted only `yoochoose-clicks.dat` using Databricks cloud compute.
- Persisted the extracted source in the Landing preparation area.
- Read the complete clickstream using Spark with an explicit source schema.
- Validated source row count: 33,003,944 records.
- Parsed and profiled event timestamps.
- Profiled 183 event dates.
- Selected daily source organization based on actual data distribution.
- Wrote prepared clickstream as headerless CSV files organized by `event_date`.
- Preserved the original four-field source payload.
- Validated 183 daily directories.
- Validated prepared payload structure.
- Read back the complete prepared dataset and confirmed 33,003,944 records.

### Key Result

YOOCHOOSE cloud preparation is complete with no record loss.

### Next Step

Design the file-based incremental ingestion mechanism:

Landing Prepared
→ File Discovery
→ File Identity
→ Manifest
→ Airflow
→ ADLS Bronze


## Mốc 21 — YOOCHOOSE Cloud Preparation Complete

### 13/08/2026

**Mục tiêu**

- Prepare the historical YOOCHOOSE clickstream dataset entirely in Azure for the FastOrder file-based ingestion flow.

**Đã thực hiện**

- Bootstrapped `yoochoose-data.7z` from the external provider to ADLS Landing using Azure Data Factory.
- Configured Databricks access to ADLS Landing using: Access Connector, Managed Identity, Azure RBAC, Unity Catalog Storage Credential, External Location, and External Volumes.
- Extracted only `yoochoose-clicks.dat` using Databricks cloud compute.
- Persisted the extracted source in the Landing preparation area.
- Read the complete clickstream using Spark with an explicit source schema.
- Validated source row count: 33,003,944 records.
- Parsed and profiled event timestamps.
- Profiled 183 event dates.
- Selected daily source organization based on actual data distribution.
- Wrote prepared clickstream as headerless CSV files organized by `event_date`.
- Preserved the original four-field source payload.
- Validated 183 daily directories and prepared payload structure.
- Read back the complete prepared dataset and confirmed 33,003,944 records.

**Kết quả**

- YOOCHOOSE cloud preparation is complete with no record loss.

**Bước tiếp theo**

- Design the file-based incremental ingestion mechanism: Landing Prepared → File Discovery → File Identity → Manifest → Airflow → 

## Mốc 22 — File-Based Bronze Writer Prototype Complete

### 15/08/2026

**Mục tiêu**

- Design and implement the File-based Bronze Writer and the skeleton for the Ingestion Runner.

**Đã thực hiện**

- Implemented File-based Bronze Writer Prototype.
- Ensured source preservation, Parquet conversion, and injection of technical metadata.
- Enforced deterministic destination paths and replay-safe overwrite semantics (`overwrite=True`).
- Created the skeleton for `file_ingestion_runner.py`.
- Defined the `FileIngestionResult` dataclass for observability (`discovered`, `processed`, `skipped`, `retried`).

**Kết quả**

- File-based Bronze Writer Prototype Complete — source preservation, Parquet conversion, technical metadata, deterministic destination và replay-safe overwrite đã được triển khai/test.

**Bước tiếp theo**

- Implement Manifest loading and File Discovery in the Runner.

## Mốc 23 — File-Based Incremental Ingestion Core Prototype Complete

### 15/08/2026

**Mục tiêu**

- Validate the end-to-end local integration of File Discovery, Manifest Manager, Bronze Writer, and Runner.

**Đã thực hiện**

- Completed full integration testing of the File Ingestion Runner.
- Verified happy path (NEW → PROCESSED).
- Verified incremental skipping (PROCESSED → SKIP).
- Verified retry mechanism (PENDING → RETRY).
- Validated crash recovery after PENDING state save.
- Validated crash recovery after Bronze write failure.
- Ensured replay-safe overwrite on ADLS.
- Cleaned up local integration tests.

**Kết quả**

- File-based Incremental Ingestion Core Prototype Complete — Discovery + Manifest + Bronze Writer + Runner đã vượt qua happy path, incremental skip và crash/retry recovery tests.

**Bước tiếp theo**

- Bắt đầu Airflow orchestration: Tạo DAG gọi `run_file_ingestion()` với manifest thật tại `state/file_based/yoochoose_manifest.json`.

## Mốc 24 — Hoàn tất luồng Weather Forecast API Ingestion

### 16/08/2026

**Mục tiêu**

- Thiết lập một luồng incremental ingestion chịu lỗi và lũy đẳng cho external Weather Forecast API (Open-Meteo).

**Đã thực hiện**

- Tích hợp thành công Open-Meteo Forecast API thực tế cho toàn bộ 5 kho hàng FastOrder.
- Bảo toàn nguyên vẹn raw JSON response song song với technical metadata của FastOrder (`metadata.json`).
- Triển khai cơ chế tạo UUID5 tất định để cố định đường dẫn Bronze đích.
- Xây dựng giao thức commit cục bộ với file `_SUCCESS` để quản lý trạng thái.
- Cấu hình kiến trúc retry 2 tầng: HTTP backoff/retry và Airflow task-level recovery.
- Xác thực thành công khả năng tự phục hồi cục bộ trong một đợt crash thực tế do rate-limit (Airflow retry đã tự động bỏ qua 4 kho đã commit và chạy tiếp 1 kho cuối).

**Kết quả**

- Weather Forecast API Flow: COMPLETE. Hệ thống đã chứng minh được độ tin cậy E2E.

**Bước tiếp theo**

- Triển khai Historical Forecast bootstrap/backfill để khởi tạo dữ liệu phân tích ban đầu.

## Mốc 25 — Hoàn tất E2E luồng Weather API Ingestion (Forecast & Historical)

### 16/08/2026

**Mục tiêu**

- Hoàn thiện toàn bộ kiến trúc API Ingestion bao gồm luồng dự báo tương lai (Forecast) và nạp dữ liệu lịch sử (Historical Forecast).

**Đã thực hiện**

- Hoàn thành End-to-End (E2E) luồng Weather Forecast.
- Hoàn thành End-to-End luồng Historical Forecast backfill.
- Áp dụng cơ chế chia nhỏ cửa sổ (30 days/window) cho tập dữ liệu 90 ngày.
- Xác thực toàn bộ các kịch bản chịu lỗi, phục hồi cục bộ và tính tất định (deterministic ID).

**Kết quả**

- External API Source (Weather) Flow: HOÀN TOÀN COMPLETE.

## Mốc 26 — Khởi động Phát triển Tầng Silver 

### 20/08/2026

**Mục tiêu**

- Xây dựng tầng Silver cho dữ liệu Weather Forecast, chuẩn bị dữ liệu chất lượng cao cho lớp phân tích.

**Đã thực hiện**

- Bắt đầu phát triển luồng Bronze → Silver với tập dữ liệu Weather Forecast.
- Định nghĩa độ chi tiết (grain) và schema cho tập dữ liệu Silver `weather_forecast_hourly`.
- Chạy prototype việc làm phẳng (flattening) mảng hourly của Open-Meteo bằng PySpark.
- Join dữ liệu phản hồi Forecast với ingestion metadata.
- Chuẩn hóa các timestamp của Forecast sang múi giờ UTC.
- Hoàn thành các bước profiling Data Quality ban đầu trên sample ingestion.
- Tái cấu trúc thiết kế Silver: chuyển từ việc viết logic trên notebook sang các module Python transformation có thể tái sử dụng.
- Bắt đầu thiết kế cơ chế Silver ingestion incremental dựa trên phép trừ giữa Bronze ingestion ID đã commit và Silver ingestion ID đã lưu.

**Kết quả**

- Quá trình chuyển đổi dữ liệu lên Silver đã có bộ khung vững chắc và khả năng trích xuất metadata chính xác.


## Mốc 27 — Hoàn tất Transformation cho Silver Weather Forecast 

### Tháng 8/2026

**Đã thực hiện:**
- Định nghĩa độ chi tiết (grain) và schema cho tập dữ liệu Silver `weather_forecast_hourly`.
- Thiết kế quá trình xử lý incremental từ Bronze → Silver.
- Triển khai cơ chế khám phá các đơn vị ingestion Bronze đã commit.
- Triển khai cơ chế phát hiện các ingestion ID đã được xử lý ở Silver.
- Triển khai cơ chế chọn lọc các ingestion pending.
- Triển khai bulk load từ Bronze cho các đơn vị ingestion Forecast đang pending.
- Trích xuất ingestion lineage từ các đường dẫn nguồn (source paths).
- Làm phẳng (flatten) các mảng hourly của Open-Meteo thành các bản ghi Forecast theo giờ.
- Đính kèm ingestion metadata sử dụng `ingestion_id`.
- Chuẩn hóa ngữ nghĩa các cột Silver.
- Chuẩn hóa timestamp của Forecast sang múi giờ UTC.
- Tái cấu trúc logic transformation thành các hàm Python có thể tái sử dụng.
- Triển khai profiling và assertion cho Data Quality.
- Hoàn thành giai đoạn transformation tạo ra Silver Candidate.

**Bước tiếp theo:** 
- Validation cho Silver transformation.

## Mốc 27 — Hoàn thành luồng Weather Forecast Bronze → Silver

### Tháng 8/2026

Hoàn thành pipeline Silver đầu tiên của dự án FastOrder sử dụng dữ liệu Open-Meteo Weather Forecast.

**Thiết kế Silver:**
- Định nghĩa tập dữ liệu `weather_forecast_hourly`.
- Định nghĩa độ chi tiết (grain): `kho hàng × snapshot dự báo × giờ dự báo`.
- Định nghĩa khóa ứng viên (candidate key): `warehouse_id + ingestion_id + forecast_time`.
- Chuẩn hóa các timestamp tầng Silver sang múi giờ UTC.

**Xử lý Tăng tiến (Incremental Processing):**
- Khám phá các ingestion Bronze đã commit thông qua file `_SUCCESS`.
- Tra cứu các ingestion ID đã được lưu tại Silver.
- Phát hiện các ingestion pending.
- Tải hàng loạt (bulk load) các file `response.json` và `metadata.json` đang pending.
- Xử lý thành công trạng thái `NO_OP` khi không có dữ liệu pending.

**Transformation:**
- Trích xuất `ingestion_id` từ các đường dẫn gốc tại Bronze.
- Làm phẳng (flatten) các mảng hourly của Open-Meteo bằng PySpark.
- Đính kèm ingestion metadata.
- Chuẩn hóa ngữ nghĩa các cột tầng Silver.
- Chuyển đổi giờ địa phương của Forecast từ `Asia/Ho_Chi_Minh` sang UTC.
- Tái cấu trúc logic transformation thành các hàm Python tái sử dụng.

**Chất lượng dữ liệu (Data Quality):**
- Triển khai kiểm tra NULL.
- Triển khai xác thực khoảng giá trị độ ẩm.
- Triển khai xác thực lượng mưa và tốc độ gió.
- Triển khai phát hiện trùng lặp độ chi tiết.
- Triển khai cơ chế assertion Data Quality theo kiểu fail-fast.

**Validation:**
- Xác thực 48 dòng mong đợi cho mỗi Forecast ingestion.
- Xác thực độ bao phủ của các pending ingestion.
- Xác thực tổng số dòng dự kiến so với thực tế.
- Xác thực sự vắng mặt của các ingestion ID không mong đợi (unexpected IDs).

**Ghi dữ liệu Silver (Silver Write):**
- Lưu trữ dữ liệu Forecast đã validate xuống ADLS Silver bằng Delta Lake.
- Kiểm chứng lại (read-back) các ingestion ID sau khi ghi.
- Chạy lại pipeline thành công với zero đơn vị pending (chạy rỗng).
- Xác nhận hành vi incremental replay trả về `NO_OP` thành công.

**Kiến trúc Notebook:**
- Giữ lại `weather_hourly` làm notebook học tập và phát triển.
- Tạo mới `weather_forecast_silver_pipeline` làm runner cho pipeline End-to-End.

**Trạng thái Mốc: HOÀN TOÀN COMPLETE**