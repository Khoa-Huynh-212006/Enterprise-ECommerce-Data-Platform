# Technical Decision Log

## D-001 — Business-first platform design

**Date:** 24–27/07/2026  
**Decision:** Bắt đầu từ FastOrder business context và KPI, không bắt đầu từ tool.

**Reason:** Tránh project trở thành demo công nghệ không có business narrative.

---

## D-002 — Olist is seed data, PostgreSQL is the operational source

**Date:** 24–28/07/2026  
**Decision:** Olist CSV chỉ khởi tạo snapshot; Faker simulator sẽ tiếp tục INSERT/UPDATE PostgreSQL.

**Reason:** Tạo nguồn OLTP có biến động để học incremental ingestion đúng ngữ cảnh.

---

## D-003 — Schema-first instead of Pandas-managed schema

**Date:** 29/07/2026  
**Decision:** Dùng `database/schema.sql`; không dùng `to_sql(if_exists="replace")` để Pandas tự tạo bảng.

**Reason:** Kiểm soát data types, constraints, naming, FK và indexes.

---

## D-004 — SQLAlchemy Core with psycopg2

**Date:** 29/07/2026  
**Decision:** Dùng SQLAlchemy Core Engine, giữ psycopg2 làm PostgreSQL driver.

**Reason:**

- Học Engine, Connection và Transaction.
- Dùng chung cho loader và simulator.
- Không cần ORM cho pipeline data engineering.

---

## D-005 — Separate SQL artifacts and Python database code

**Date:** 29/07/2026  
**Decision:**

```text
database/      → schema.sql và SQL artifacts
fastorder/db/  → connection.py, init_db.py
```

**Reason:** Trách nhiệm rõ ràng và tránh `database/` trùng nghĩa.

---

## D-006 — Thin Airflow DAGs

**Date:** 29/07/2026  
**Decision:** Logic tái sử dụng nằm trong `fastorder/`; DAG chỉ orchestration.

**Reason:** Dễ test, dễ tái sử dụng, tránh DAG file phình to.

---

## D-007 — No `is_simulated` or `simulation_id`

**Date:** 29/07/2026  
**Decision:** Không đánh dấu record simulator trong operational tables.

**Reason:** Simulator được xem là producer của operational data; downstream không cần phân biệt seed và simulated record.

---

## D-008 — Atomic initial bootstrap

**Date:** 29/07/2026  
**Decision:** Initial Olist loader dùng một transaction cho 9 bảng.

**Reason:** Đảm bảo snapshot nhất quán và tránh partial initial state.

**Boundary:** Airflow incremental sau này sẽ retry/checkpoint theo entity, không dùng một transaction khổng lồ.

---

## D-009 — Surrogate primary key for geolocation

**Date:** 29/07/2026  
**Decision:** Không dùng ZIP/lat/lng làm composite primary key.

**Reason:** Source duplicate và precision rounding có thể vi phạm uniqueness; không có business rule xác nhận natural key.

---

## D-010 — Timestamp-based incremental extraction

**Date:** 29/07/2026  
**Decision:** Dự kiến Airflow đọc thay đổi bằng `updated_at`.

**Terminology:** Đây là timestamp-based incremental extraction, không phải log-based CDC.

**Implementation note:** UPDATE phải cập nhật `updated_at` rõ ràng hoặc dùng trigger.

---

## D-011 — Documentation must evolve with implementation

**Date:** 29/07/2026  
**Decision:** Mỗi thay đổi phải kết thúc bằng “Docs cần cập nhật” hoặc “Không cần cập nhật docs”.

**Reason:** Ngăn architecture, schema và runbook trở nên lạc hậu.

---

## D-012 — Separation of Ingestion Validation and Business Data Quality

**Date:** 30/07/2026  
**Decision:** Các quy tắc kiểm tra chất lượng dữ liệu nghiệp vụ (business data-quality rules) sẽ được thực hiện ở lớp Transformation (Bronze → Silver), không nằm trong initial loader.

**Reason:** 
- Initial loader chỉ chịu trách nhiệm cho Ingestion Fidelity và Structural Integrity (giữ nguyên bản dữ liệu thô, không fill NULL, không xóa bản ghi lỗi).
- Bảo vệ dữ liệu gốc tại lớp Bronze để đảm bảo Data Lineage và phục vụ Root Cause Analysis về sau. Pipeline nạp dữ liệu cũng nhờ đó mà nhẹ nhàng và dễ bảo trì hơn.
---

## D-013 — Synthetic Sparse Inventory Strategy

**Date:** 30/07/2026  
**Decision:** Sử dụng chiến lược phân bổ tồn kho thưa (Sparse Inventory) và sinh số lượng ngẫu nhiên có kiểm soát (Fixed Random Seed).  
**Quy tắc phân bổ:**
- Mỗi sản phẩm có mặt tại đúng 1 kho chính (Main warehouse: WH_HN hoặc WH_HCM) với số lượng 50–200.
- Mỗi sản phẩm có mặt tại đúng 1 kho vùng (Regional warehouse: WH_HP, WH_DN, hoặc WH_CT) với số lượng 20–100.
- 20% xác suất sản phẩm có mặt ở kho thứ ba (trong số các kho còn lại) với số lượng 10–50.

**Reason:** 
- Tránh việc phân bổ tất cả sản phẩm vào tất cả các kho (Dense Inventory), gây ra sự thiếu thực tế về mặt nghiệp vụ logistics và làm phình to database không cần thiết (chỉ tạo ~72.000 records thay vì ~164.000 records).
- Việc dùng Fixed Seed (`random.seed(42)`) đảm bảo tính tái lập (reproducibility) khi reset DB, hỗ trợ tốt nhất cho quá trình debug, test và data validation.

```markdown
---

## D-013 — Synthetic Sparse Inventory Strategy

**Date:** 30/07/2026  
**Decision:** 
- Áp dụng chiến lược Sparse Inventory, phân bổ mỗi sản phẩm có mặt tại 2–3 warehouse.
- Sử dụng Fixed Seed (`random.seed(42)`).
- Sử dụng mệnh đề `ON CONFLICT DO NOTHING` cho quá trình seed inventory.

**Reason:** 
- Tạo ra phân phối dữ liệu thực tế hơn so với việc seed tất cả sản phẩm vào tất cả kho.
- Fixed seed đảm bảo tính nhất quán (reproducible) của dữ liệu mô phỏng qua các lần thiết lập môi trường.
- `ON CONFLICT DO NOTHING` là chốt chặn an toàn cốt lõi: nó ngăn việc ghi đè trạng thái vận hành (operational inventory) nếu script seed vô tình bị chạy lại trong lúc hệ thống đang có dữ liệu giao dịch động.

---

## D-014 — Faker Simulator: State Machine & Quantity Logic

**Date:** 30/07/2026  
**Decision:** 
1. **Event CREATE_ORDER:** Khởi tạo order với status mặc định là `created`. Payment được sinh đồng thời trong cùng một transaction cùng với việc trừ số lượng (`quantity`) vào bảng `inventory`.
2. **Event ADVANCE_ORDER_STATUS:** Không random trạng thái lộn xộn, mà chỉ tịnh tiến (UPDATE) ngẫu nhiên theo đúng luồng State Machine (ví dụ: `created` -> `approved` -> `processing` -> `shipped` -> `delivered`).
3. **Item Quantity:** Số lượng từng item được chọn ngẫu nhiên trong khoảng `[1, quantity_available]`.

**Reason:** 
- Đảm bảo tính toàn vẹn dữ liệu: Order sinh ra là phải thu tiền và trừ kho ngay lập tức, nếu lỗi 1 bước sẽ Rollback toàn bộ.
- Tuân thủ State Machine giúp Pipeline Airflow Incremental sau này có cơ hội "bắt" (capture) được các sự kiện UPDATE thay đổi trạng thái theo thời gian thực (CDC mô phỏng).

---

## D-015 — Faker Simulator: Concurrency Control & B2C Limit

**Date:** 30/07/2026  
**Decision:** 
1. **Quantity Cap:** Giới hạn số lượng mua tối đa cho mỗi mặt hàng trong một đơn là 10 (hoặc bằng tồn kho hiện tại nếu tồn kho < 10) để phản ánh đúng hành vi mua sắm B2C.
2. **Concurrency Handling:** Không sử dụng `executemany` cho lệnh `UPDATE inventory` vì driver `psycopg2` không cam kết trả về `rowcount` chính xác trong chế độ này. Thay vào đó, Simulator duyệt vòng lặp thực thi từng lệnh `UPDATE` kèm điều kiện `quantity_available >= :buy_qty` và kiểm tra `rowcount == 1`. 

**Reason:** 
- Đảm bảo tính chân thực của dữ liệu mô phỏng.
- Kỹ thuật Optimistic Locking (kiểm tra rowcount) khóa chặt lỗ hổng Overselling khi chạy nhiều tiến trình Simulator song song, nếu có tranh chấp tài nguyên (race condition), transaction sẽ lập tức Rollback.

## D-016 — Architecture of Simulator Runner

**Date:** 01/08/2026  
**Decision:** 
- Triển khai Simulator Runner hỗ trợ chạy hữu hạn (Bounded) hoặc liên tục (Continuous).
- Mỗi cycle tạo 0-3 orders và gọi status updater.
- Áp dụng nguyên tắc Fail-fast, không che giấu exception.
- Hỗ trợ graceful shutdown bằng Ctrl+C.
- Chạy independent validator ở cuối phiên.

**Reason:** 
- Bounded mode bảo đảm có test data phục vụ quá trình test nhanh, trong khi Continuous mode mô phỏng operational traffic dài hạn.
- Nguyên tắc Fail-fast giúp phát hiện ngay lỗi Database/Logic thay vì chạy lặp vô hạn. Graceful shutdown ngăn chặn treo transaction.
- Independent validator chốt chặn chất lượng dữ liệu cuối phiên đảm bảo dữ liệu sinh ra không vi phạm Data Invariants (12/12 rules PASS).

## D-017 — At-Least-Once Bronze Ingestion & Timestamp-based Extraction

**Date:** 05/08/2026
**Decision:**
*   Sử dụng Timestamp-based Incremental Extraction thay vì WAL-based CDC (Debezium/Kafka) cho giai đoạn MVP.
*   Chốt Watermark theo cơ chế Composite Key: `(updated_at, order_id)`.
*   Chấp nhận Delivery Semantics là **At-least-once** từ PostgreSQL xuống Bronze. 
*   Việc khử trùng lặp (Deduplication) sẽ được thực hiện ở lớp Silver.

**Reason:**
*   Hệ thống file local không hỗ trợ distributed transaction. Việc giả lập Exactly-once delivery ở bước này là quá rườm rà và dễ phát sinh lỗi (error-prone). 
*   Medallion Architecture sinh ra là để các lớp san sẻ gánh nặng cho nhau. Bronze có nhiệm vụ lấy dữ liệu nhanh nhất và an toàn nhất (append-only), còn Silver xử lý logic nghiệp vụ (Deduplicate).
*   Upper Watermark giúp cô lập batch dữ liệu, tránh việc query đuổi theo dữ liệu do Simulator sinh ra liên tục.

## D-018 — Timestamp Honesty & Schema Enforcement for Incremental Extraction

**Date:** 05/08/2026
**Decision:**
*   Bảo tồn định dạng `TIMESTAMP WITHOUT TIME ZONE` cho checkpoint JSON (dạng naive ISO 8601, không gắn cờ `Z` UTC).
*   Thực thi cứng ràng buộc `NOT NULL` cho cột `updated_at` trong database.
*   Thiết lập Composite Index `(updated_at, order_id)` trực tiếp trên schema.

**Reason:**
*   Ép múi giờ UTC trong code Python đối với một database không lưu múi giờ sẽ tạo ra metadata giả, gây lỗi lệch pha hệ thống (offset drift) về sau. Phải trung thực với schema nguồn.
*   Giá trị `updated_at = NULL` sẽ vĩnh viễn lọt lưới Incremental Query, nên `NOT NULL` là lá chắn bắt buộc.
*   Composite Index là thành phần vật lý không thể thiếu để duy trì hiệu năng khi query quét theo watermark ngày càng phình to.

## D-019 — Idempotent Overwrite for Bronze Layer (Extraction ID Reuse)

**Date:** 06/08/2026
**Decision:**
*   Lựa chọn Phương án B (Idempotent Semantics) cho kiến trúc ghi file tại tầng Bronze. 
*   Quá trình retry (chạy lại do lỗi) sẽ dùng lại cùng một `extraction_id` cho cùng một lô công việc.
*   Thực hiện ghi đè an toàn thông qua hàm `os.replace()` (Atomic Replace).

**Reason:**
*   Ngăn chặn sự tích tụ của các thư mục/file Parquet rác trong Data Lake khi tiến trình crash giữa bước "Ghi Parquet" và "Commit Checkpoint". 
*   Dù tầng Silver có năng lực Deduplicate, việc giữ sạch tầng vật lý Bronze ngay từ đầu (giảm thiểu số lượng file trùng lặp) là tiêu chuẩn công nghiệp tốt nhất, giúp giảm tải IO và tránh phải xây dựng các kịch bản dọn rác (Garbage Collection/Vacuum) phức tạp.

## D-020 — Use Pending Batch Context for Stable Crash Recovery

**Date:** 07/08/2026
**Decision:**
* Bổ sung cơ chế `Pending Batch Context` lưu dưới dạng JSON atomic để theo dõi trạng thái của batch đang chạy dở.
* Kế thừa chặt chẽ `run_id`, `ingested_at`, `batch_size`, và ranh giới watermark (lower/batch_upper) từ Pending Context khi tiến hành phục hồi sau sự cố.

**Reason:**
* Checkpoint chỉ trả lời câu hỏi "Pipeline đã hoàn thành đến đâu?", nhưng không biết "Tiến trình đang làm dở việc gì?". 
* Việc không có Pending Context sẽ khiến tiến trình khi restart bị mất `extraction_id` cũ, tự động sinh ID mới và ghi file Parquet mới, dẫn đến rác dữ liệu trên Data Lake hoặc ghi đè sai batch. Pending Context đảm bảo ranh giới dữ liệu và danh tính của lần chạy (Stable Run Identity) được bảo toàn tuyệt đối xuyên suốt các lần khởi động lại tiến trình.

## D-021 — Áp dụng Stable Retry Context và Fail-Fast cho State Management

**Date:** 2026-08-07
**Decision:**
* Sử dụng `Pending Batch Context` để kế thừa `run_id`, `ingested_at`, `batch_size`, và watermark boundaries khi phục hồi sau sự cố.
* Tuyệt đối không dùng code để tự động lấp liếm (ví dụ: tự xóa file checkpoint rỗng hoặc tự đoán extraction_id). Hệ thống phải Fail-fast khi phát hiện trạng thái state file bất thường.

**Reason:**
* Đảm bảo ranh giới dữ liệu và danh tính của lần chạy (Stable Run Identity) không bị biến đổi xuyên suốt các lần restart.
* Việc tự động bỏ qua lỗi của file trạng thái (như file bị rỗng do I/O error) có thể dẫn đến hậu quả nghiêm trọng như kéo lại toàn bộ lịch sử dữ liệu (Disaster Risk) hoặc ghi đè sai batch. Con người phải can thiệp khi State files bị hỏng.

## D-022 — Sử dụng Airflow Context cho Stable Run Identity và Fail-Fast Mount Guards

**Date:** 2026-08-10
**Decision:**
*   Sử dụng `dag_run.start_date` thay vì `logical_date` để biểu diễn chính xác thời điểm thực sự bắt đầu tiến trình Ingestion.
*   Làm sạch (Sanitize) `dag_run.run_id` thành định dạng an toàn cho File System trước khi dùng để tạo `extraction_id`.
*   Sử dụng cơ chế Fail-Fast (raise Exception) thay vì tự động `mkdir()` cho các đường dẫn dữ liệu trong container.

**Reason:**
*   Đảm bảo tính Deterministic của tiến trình khi chạy trên Airflow. `run_id` mặc định của Airflow chứa các ký tự đặc biệt có thể phá hỏng tiến trình ghi file Parquet trên môi trường Windows bind mount.
*   Bảo vệ Data Pipeline khỏi tình trạng chạy thành công giả (Fake success) khi mount bị lỗi, giúp phát hiện sớm các vấn đề về cơ sở hạ tầng.

## D-023 — Hoàn tất Local Airflow MVP và Tiếp cận ADLS theo hướng Probe-First

**Date:** 2026-08-10
**Decision:**
*   Chính thức nghiệm thu giai đoạn Local Airflow Incremental Ingestion MVP sau khi PASS 3 kịch bản E2E.
*   Bước tiếp theo là tích hợp Azure Data Lake Storage (ADLS Gen2) cho tầng Bronze.
*   Quyết định áp dụng chiến lược "Probe-First": Xây dựng và test độc lập module `adls_client.py` để xác thực Azure Authentication và quyền I/O trước khi tích hợp vào Bronze Writer và Incremental Runner.

**Reason:**
*   Việc phân lập hạ tầng (Infrastructure Isolation) giúp tách bạch các lỗi liên quan đến Mạng/Bảo mật (Azure Auth, RBAC) khỏi các lỗi liên quan đến Data Logic (Runner, Airflow DAG). Nếu hệ thống crash, ta biết chính xác nguyên nhân nằm ở Data Layer hay Transport Layer.

## D-024 — Chuyển đổi Timezone trong DAG trước khi sinh Partition

**Date:** 2026-08-11
**Decision:**
*   Convert `dag_run.start_date` từ chuẩn UTC (Airflow default) sang múi giờ nghiệp vụ (`Asia/Ho_Chi_Minh`), sau đó mới gỡ bỏ timezone (naive datetime) để truyền vào Incremental Runner.

**Reason:**
*   Đảm bảo metadata partitioning trên ADLS (`ingestion_date=YYYY-MM-DD`) đồng nhất tuyệt đối với ngày vận hành kinh doanh (Business Convention) tại Việt Nam. Nếu chỉ gọi `.replace(tzinfo=None)` trên UTC time, dữ liệu của phiên chạy đầu ngày tại VN có thể bị rơi nhầm vào partition của ngày hôm trước do lệch múi giờ.

## D-025 — Cleanup Bronze Root và Giao phó I/O cho ADLS Client

**Date:** 2026-08-11
**Decision:**
*   Xóa bỏ tham số `bronze_root` xuyên suốt toàn bộ Ingestion Runner và Orchestration DAG, đẩy việc cấu hình URL/Path về tay ADLS Client và biến môi trường.

**Reason:**
*   Loại bỏ dư thừa kỹ thuật (Tech Debt) của giai đoạn Local MVP, đảm bảo kiến trúc tuân thủ nguyên tắc Separation of Concerns. DAG và Runner không cần quan tâm Storage vật lý nằm ở đâu.

## D-026 — Lựa chọn Kiến trúc Cloud-Native File Ingestion và Time-Based Partitioning

**Date:** 2026-08-11
**Decision:**
*   **Loại bỏ Local Processing:** Khai tử kiến trúc "Tải file về Laptop -> Xử lý/Split local -> Đẩy lên Cloud". Thay vào đó, áp dụng luồng dữ liệu 100% Cloud-to-Cloud.
*   **Sử dụng Azure Data Factory (ADF):** Dùng ADF Copy Activity để kéo trực tiếp file gốc (ví dụ: `.7z`) từ External HTTP Endpoint thả thẳng vào thư mục `landing/bootstrap/` trên ADLS Gen2.
*   **Time-Based Partitioning:** Dữ liệu Clickstream (YOOCHOOSE) sẽ được chuẩn bị (Cloud Preparation) và chia file dựa trên `event_date` thay vì chia theo số dòng cố định (VD: 250k rows/file).

**Reason:**
*   Việc dùng máy tính cá nhân làm trạm trung chuyển không có khả năng mở rộng (Scalability) và không phản ánh đúng thực tế doanh nghiệp. ADF là công cụ Managed Service tối ưu nhất để chịu tải tác vụ data movement từ bên ngoài vào Cloud Landing Zone.
*   Dữ liệu thực tế luôn đến theo cửa sổ thời gian (Time window). Phân vùng dữ liệu theo `event_date` sát với nghiệp vụ hơn, đồng thời tối ưu hóa quá trình đọc dữ liệu của Apache Spark ở các layer sau (Bronze -> Silver).

## D-027 — Phân tách trách nhiệm I/O trong File-based Ingestion

**Date:** 2026-08-12
**Decision:**
*   Phân định rõ ràng vai trò của 3 hệ thống: ADF (chuyên chở data ngoài vào Landing), Databricks (giải nén, chuẩn bị cấu trúc time-based tại Landing), và Airflow (Ingest từ Landing vào Bronze).

**Reason:**
*   Ngăn chặn Airflow Worker phải chịu tải các tác vụ không phù hợp (giải nén file gigabytes, xử lý 33 triệu rows). Airflow chỉ nên làm Orchestrator. Trả các tác vụ Heavy Compute (Extract, Partitioning) về cho nền tảng Distributed Compute đúng nghĩa là Spark/Databricks.

## D-028 — Xác thực Storage cho Databricks qua Managed Identity

**Date:** 2026-08-12
**Decision:**
*   Sử dụng Azure Access Connector / Managed Identity kết hợp với Azure RBAC để cấp quyền cho Azure Databricks truy cập ADLS Gen2 (`fastorderdatalake`). 
*   **Quyền hạn MVP:** Cấp role `Storage Blob Data Contributor` cho Databricks Managed Identity ở scope cụ thể (container `landing`).

**Reason:**
*   Thực hành Security Best Practice theo khuyến nghị của Microsoft và kiến trúc Unity Catalog. Chấm dứt việc nhúng `client_secret` hay `account_key` vào mã nguồn Spark Conf, ngăn ngừa rủi ro rò rỉ credential và giảm chi phí vận hành xoay vòng khóa (key rotation).

## D-029 — YOOCHOOSE Landing Preparation Strategy

**Date:** 13/08/2026  
**Context:** YOOCHOOSE clickstream is distributed as a compressed `yoochoose-data.7z` archive containing approximately 33 million click events. The file-based ingestion flow requires a cloud-native source boundary without relying on the developer's local filesystem.

**Decision:** 
Use the following preparation flow:
External YOOCHOOSE Provider → Azure Data Factory → ADLS Landing / bootstrap → Azure Databricks → ADLS Landing / preparation → ADLS Landing / prepared

Landing structure:
landing/
└── clickstream/
    └── yoochoose/
        ├── bootstrap/
        │   └── yoochoose-data.7z
        ├── preparation/
        │   └── yoochoose-clicks.dat
        └── prepared/
            └── event_date=YYYY-MM-DD/
                └── *.csv

The prepared dataset uses:
- CSV / comma-delimited text
- no header
- original four source fields (session_id, event_timestamp, item_id, category)
- `event_date` is used only for physical organization and is not added to the file payload.

Daily organization was selected after profiling the actual dataset:
- 183 days
- average: 180,349 events/day
- minimum: 2,220 events/day
- maximum: 393,950 events/day

**Reason:** Landing should preserve upstream data as closely as practical. Converting the prepared files to Parquet would introduce an unnecessary representation change before the FastOrder ingestion pipeline accepts the source. Daily file organization provides a business-time-based source delivery layout while avoiding arbitrary row-count chunking such as the previous 250,000-rows-per-file experiment. Hourly organization was rejected because it would create unnecessary small source files for this dataset.

**Consequences:** 
- The previous 133-file local splitter remains an experiment only.
- Laptop/local filesystem is not part of the official ingestion architecture.
- `preparation/` is an intermediate cloud area.
- `prepared/` becomes the source boundary for Airflow file-based ingestion.
- Analytical/Silver partitioning will be decided independently from Landing layout.

## D-030 — File Manifest Storage Strategy for MVP

**Date:** 13/08/2026  
**Decision:** File Manifest MVP uses shared atomic JSON state; dedicated PostgreSQL metadata store deferred to future scale.

**Reason:** The current source operates at a scale of a few hundred physical files, and the Airflow File DAG will execute as a single active run. The `state/` directory is already a persistent shared volume, and the atomic JSON pattern from the Database Ingestion MVP can be directly reused. This fulfills the MVP requirement without introducing additional Azure or Database infrastructure complexities at this stage.

## D-031 — File-Based Bronze Writer Architecture and Runner Observability

**Date:** 15/08/2026  
**Context:** Developing the file-based ingestion pipeline merging Discovery, Manifest Manager, and Bronze Writer.  
**Decision:** 
- Bronze Writer strictly preserves the original four source fields, adds technical metadata, converts to Parquet, and writes to a deterministic destination with replay-safe overwrite.
- The File Ingestion Runner handles orchestration, not data manipulation.
- Logical ingestion timestamp (`ingested_at`) is defined as the moment a file is first discovered (`discovered_at` in the manifest), not when the write operation finishes.
- Observability metrics (`discovered`, `processed`, `skipped`, `retried`) are integrated directly into the runner.  

**Reason:** Preserving the `discovered_at` timestamp ensures that retries maintain the exact same ingestion ID and logical time, guaranteeing deterministic output paths and idempotent overwrites in Bronze. Differentiating between `retried` (an attempt state) and `processed` (a success state) provides clear operational visibility for Airflow logs.


## D-032 — Airflow Orchestration for File-Based Ingestion

**Date:** 15/08/2026  
**Context:** The core components for file-based ingestion (Discovery, Manifest Manager, Bronze Writer, and Runner) have been fully implemented and have passed integration testing for both happy paths and failure scenarios.

**Decision:** 
Proceed to Airflow orchestration by creating a DAG that directly calls `run_file_ingestion()` using the real state manifest at `state/file_based/yoochoose_manifest.json`.

**Reason:** The core prototype successfully validated all critical resilience requirements:
- NEW → PROCESSED
- PROCESSED → SKIP
- PENDING → RETRY
- Crash recovery (after PENDING and after Bronze write)
- Replay-safe overwrite semantics

**Consequences:** 
- The local integration testing phase is complete and cleaned up.
- The next step focuses entirely on Airflow DAG development and containerized execution.

## D-033 — Weather API Sourcing Strategy (Forecast vs. Historical)

**Date:** 16/08/2026  
**Context:** FastOrder requires weather data to support logistics operations (predicting delivery delays) and analytics (correlating past delays with weather conditions). Open-Meteo provides Forecast, Historical Forecast, and Historical Weather (ERA5) APIs.

**Decision:** 
- **Forecast API:** Chosen as the primary source for ongoing, incremental operational ingestion (periodic fetching for the 5 warehouses).
- **Historical Forecast API:** Chosen for the initial bootstrap/backfill (e.g., past 30–90 days) to populate the database for immediate analytical value.
- **Historical Weather ERA5:** Excluded from the MVP scope.

**Reason:** FastOrder needs near-real-time logistics forecasting and recent operational analysis, not multi-decade climate trend research. The Historical Forecast API shares the same parameters and response structure as the Forecast API, allowing for a clean, unified codebase (`fetch_current_forecast` vs. `fetch_historical_forecast`) without building two separate ingestion systems. This perfectly mirrors the project's database architecture (initial bootstrap followed by incremental delta loads).

**Consequences:** 
- The Weather API Client will be designed to support both current and historical forecast endpoints using a shared data contract.
- The platform gains immediate analytical utility from the backfilled weather data, avoiding the "cold start" problem of waiting months for forecast data to accumulate.

## D-034 — Weather API Bronze Storage Strategy (Sidecar Pattern)

**Date:** 16/08/2026  
**Context:** Storing Open-Meteo API responses in the Bronze layer requires a mechanism that preserves the raw provider data while tracking FastOrder's internal ingestion lineage.

**Decision:** 
API Bronze preserves Open-Meteo responses as raw JSON snapshots (`response.json`). FastOrder technical metadata (e.g., `ingestion_id`, `warehouse_id`, `requested_at`) is stored in a separate companion file (`metadata.json`) alongside the payload, rather than modifying or flattening the provider's original JSON structure.

**Reason:** Injecting internal metadata directly into the API payload alters the raw semantics and structure of the source data. The Sidecar pattern (`response.json` + `metadata.json`) guarantees 100% preservation of the provider's original keys, nested arrays, and units, while maintaining clean and isolated technical lineage for downstream Silver layer processing.

**Consequences:** 
- Each API ingestion unit corresponds to a specific directory structure: `bronze/weather/open_meteo/forecast/ingestion_date=.../warehouse_id=.../ingestion_id=.../`.
- The Weather API Client will return pure Python dictionaries (parsed JSON) instead of Pandas DataFrames.

## D-035 — Tính lũy đẳng (Idempotency) của API Ingestion và Chiến lược Retry 2 tầng

**Date:** 16/08/2026  
**Context:** Luồng API Weather Forecast cần khả năng chịu lỗi (fault tolerance) và tính lũy đẳng (idempotency) để xử lý sự cố mạng ngắn hạn (như rate limit của Open-Meteo) mà không làm nhân bản dữ liệu trên Data Lake hay phải phụ thuộc vào một global state manifest cồng kềnh.

**Decision:** 
- **Định danh tất định (Deterministic Identity - UUID5):** `ingestion_id` được tạo bằng UUID5 dựa trên khóa logic (`api_type|warehouse_id|run_id`) để đảm bảo các lần chạy logic giống nhau luôn ghi vào đúng một đường dẫn Bronze.
- **Giao thức Commit:** File đánh dấu `_SUCCESS` chỉ được ghi duy nhất vào bước cuối cùng của một API ingestion unit.
- **Chiến lược Retry 2 tầng:** Áp dụng HTTP-level retry (với backoff) cho các lỗi mạng chập chờn, và phụ thuộc vào Airflow task retry cho các lỗi hệ thống kéo dài.

**Reason:** UUID4 sẽ tạo ID mới mỗi khi retry, gây rác dữ liệu. UUID5 trói buộc đường dẫn đích với một Airflow run cụ thể, cho phép ghi đè an toàn (overwrite). Marker `_SUCCESS` loại bỏ sự phụ thuộc vào một JSON state file tập trung, giúp kiểm tra idempotency cục bộ mà không bị khóa (lock-free). Nhờ vậy, nếu một run bị crash giữa chừng, lần retry của Airflow sẽ dễ dàng BỎ QUA (SKIP) các unit đã commit và chỉ chạy tiếp những phần còn thiếu.

**Consequences:** 
- Tích hợp end-to-end thành công với khả năng phục hồi cục bộ (ví dụ: phục hồi sau crash bằng cách skip 4 kho hàng đã commit và ingest chính xác 1 kho còn lại).

## D-036 — Historical Forecast Backfill Strategy

**Date:** 16/08/2026  

**Decision:** 
Historical Forecast bootstrap = 90 days, split into 30-day deterministic windows.
Forecast ongoing = 48h future window, incremental periodic.

**Why:**
- Isolate failure (lỗi ở khoảng thời gian nào chỉ cần chạy lại khoảng đó).
- Retry individual windows một cách độc lập và an toàn.
- Avoid overlapping rolling 90-day backfills (ngăn chặn việc backfill trôi dạt theo ngày chạy thực tế).
- Deterministic replay (đảm bảo tính tất định khi Airflow chạy lại các khoảng thời gian trong quá khứ).