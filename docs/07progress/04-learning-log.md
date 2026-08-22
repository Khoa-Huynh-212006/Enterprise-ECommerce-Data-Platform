## 07/08/2026 - Crash Recovery & State Management In Data Pipelines

**1. Ranh giới giữa Checkpoint và Pending Context**
Trong thiết kế Data Pipeline, Checkpoint đóng vai trò là "Nguồn sự thật" (Source of Truth) cho những dữ liệu đã được xác nhận an toàn. Trong khi đó, Pending Context đóng vai trò là "Bộ nhớ ngắn hạn" lưu lại ý định thực thi. Phải có cả hai mới giải quyết được bài toán Crash Recovery mà không tạo ra bản ghi nhân bản (duplicates).

**2. Sức mạnh của Idempotent Overwrite**
Cơ chế Atomic Write (`os.replace`) kết hợp với việc tái sử dụng `extraction_id` từ Pending Context giúp kiến trúc không cần phải lo lắng về việc dọn dẹp file rác nếu tiến trình sập giữa chừng. Chạy lại (Retry) đồng nghĩa với việc ghi đè lên đúng chỗ cũ một cách an toàn.

**3. Phân tích Failure Windows (Cửa sổ rủi ro)**
Không thể test hệ thống bằng cách chỉ cho nó chạy thành công. Phải dùng `unittest.mock` để chủ động cắt đứt tiến trình (raise Exceptions) tại các vị trí nhạy cảm nhất (giữa các thao tác I/O). Việc chia nhỏ thứ tự thực thi thành 5 bước độc lập cho phép cô lập lỗi và xử lý triệt để mọi trường hợp mất điện hay tắt nguồn máy chủ.

## 2026-08-07 - Stable Boundary, Fail-Fast và Crash Recovery

**1. Stable Boundary và Run Identity**
Khi phục hồi từ một sự cố, tiến trình không được phép tự do lấy cấu hình mới (như `batch_size` mới hay `run_upper_watermark` mới từ DB). Nó phải tuyệt đối trung thành với bối cảnh đã lưu trong `Pending Context`. Việc này đảm bảo tính vẹn toàn của dữ liệu, không tạo ra khoảng trống hoặc sự trùng lặp.

**2. Triết lý Fail-Fast với State Management**
Trong Data Engineering, việc âm thầm bỏ qua lỗi (như file trạng thái 0 bytes) là con đường ngắn nhất dẫn đến thảm họa. Lỗi `JSONDecodeError` khi đọc checkpoint hỏng là một rào chắn bảo vệ hệ thống khỏi việc kéo lại toàn bộ hàng chục triệu records từ đầu. Code tuyệt đối không được tự động can thiệp hay xóa state file hỏng.

**3. Idempotent Overwrite và Failure Windows**
Việc chia quá trình ingest thành 5 bước I/O rõ ràng (Extract -> Pending -> Write Bronze -> Checkpoint -> Delete Pending) cô lập hoàn toàn các "Cửa sổ rủi ro" (Failure Windows). Khi kết hợp với tính năng Idempotent Overwrite (chấp nhận ghi đè an toàn), hệ thống trở nên vững chắc trước các tình huống tắt nguồn hay ngắt kết nối đột ngột.


## 13/08/2026 - File-Based Cloud Preparation

**1. Landing vs Bronze**
Landing represents files accepted from or prepared on the source side before FastOrder ingestion. Bronze represents data that has already been accepted by the FastOrder ingestion pipeline. Therefore: Landing != Bronze.

**2. Data Movement vs Processing vs Orchestration**
ADF is for external/cloud data movement. Databricks/Spark is for large-scale data preparation. Airflow is for platform orchestration. Each tool has a different responsibility and should not automatically replace the others.

**3. Cloud Identity**
Databricks accesses ADLS through: Databricks → Unity Catalog → Storage Credential → Access Connector → Managed Identity → Azure RBAC → ADLS. Azure RBAC controls physical storage access, while Unity Catalog controls governed Databricks access.

**4. Temporary Compute Storage**
`/local_disk0` is temporary storage attached to Databricks compute. Using it for archive extraction does not make the developer laptop part of the data architecture. Persistent data must be written back to cloud storage.

**5. Raw Data Preservation**
Landing preparation should preserve upstream data as closely as practical. For YOOCHOOSE: original four fields are preserved, no header is introduced, no business cleaning is performed, no deduplication is performed, and no enrichment is performed. `event_date` is used only for physical routing.

**6. Partition vs File**
`event_date=2014-08-10/` is a logical filesystem partition. It does not inherently mean that exactly one physical file must exist inside the directory. Spark execution partitions and filesystem partitions are different concepts.

**7. Data-Driven File Layout**
The initial 250,000-row chunking strategy was arbitrary. The final source layout was chosen after profiling actual event-time distribution. Observed: 183 days, ~180k events/day average, ~394k events/day maximum. Daily organization was therefore chosen over hourly organization.

**8. Spark Execution**
Spark transformations such as `select()` and `repartition()` are lazy. Actual processing is triggered by actions such as `count()`, `display()`, or `write`. `repartition("event_date")` affects Spark execution layout, whereas `partitionBy("event_date")` affects output filesystem layout.

## 15/08/2026 - File Ingestion Orchestration

**1. Runner Responsibilities**
The Runner is an orchestrator. It does not parse CSV, serialize Parquet, or manipulate JSON directly. It coordinates the File Discovery, Manifest Manager, and Bronze Writer modules.

**2. Logical Ingestion Timestamp**
`discovered_at` is defined as the time logical ingestion begins. `processed_at` is the time ingestion completes. The runner passes `ingested_at=entry.discovered_at` to the Bronze Writer. This guarantees that retries reuse the same `ingestion_id` and timestamp, ensuring idempotent Bronze paths.

**3. Observability Metrics**
- `discovered`: Total valid source files found in Landing.
- `skipped`: Files already marked as PROCESSED in the manifest.
- `retried`: Files found in PENDING state (from a previous failure/crash) that are attempted again.
- `processed`: Files successfully written to Bronze in the current run (includes successful retries).
A single file can increment both `retried` and `processed` without contradiction.

## 15/08/2026 - Incremental File Ingestion Integration

**1. State Transition Safety**
The integration of Discovery, Manifest, and Bronze Writer solidifies the idempotent nature of the pipeline. The state transitions (NEW → PROCESSED, PROCESSED → SKIP, PENDING → RETRY) ensure that data is never duplicated, even if the process is executed multiple times over the same Landing files.

**2. Crash Recovery Semantics**
By saving the PENDING state before any heavy I/O operations (like downloading or uploading to ADLS), the pipeline establishes a critical recovery point. A crash after PENDING or after a partial Bronze write simply results in a RETRY on the next run, which safely overwrites the deterministic Bronze path and eventually marks the file as PROCESSED.

## 16/08/2026 - Weather API Ingestion Concepts

**1. Technical Contract Validation**
Metadata builder hoạt động như một bức tường lửa bảo vệ contract. Việc validate chặt chẽ từ gốc (ví dụ: timezone-aware timestamps, missing coordinates) giúp hệ thống Fail-Fast trước khi tiến hành các tác vụ I/O đắt đỏ.

**2. Forecast vs Historical Forecast**
Sử dụng Forecast cho vận hành logistics tương lai (Incremental). Sử dụng Historical Forecast cho backfill dữ liệu phân tích (Bootstrap). Không sử dụng Historical Weather (ERA5) để tránh over-engineering vì FastOrder không phân tích khí hậu nhiều thập kỷ.

**3. logical_at vs requested_at**
`logical_at` cố định theo thời gian schedule của Airflow, dùng để tạo UUID5 và Partition, đảm bảo tính tất định (Deterministic) khi retry. `requested_at` là thời gian gọi HTTP thực tế, chỉ dùng để audit.

**4. UUID5 & Commit Marker (`_SUCCESS`)**
UUID5 băm các thuộc tính logic (api_type, warehouse_id, run_id) để tạo ra ID không đổi qua các lần retry. File `_SUCCESS` đóng vai trò là cờ báo hiệu hoàn tất, giúp hệ thống biết chính xác điểm cần phục hồi (Retry-safe destination) mà không cần global JSON manifest.

**5. HTTP Retry vs Airflow Retry**
HTTP backoff hấp thụ các lỗi mạng chớp nhoáng (như Rate limit). Airflow retry gánh vác các lỗi hệ thống hoặc downtime kéo dài.

**6. Backfill Window**
Chia nhỏ dữ liệu lịch sử (ví dụ: 90 ngày thành các khoảng 30 ngày) giúp khoanh vùng rủi ro lỗi và tối ưu memory/API payload.

## 20/08/2026 - Tầng Silver — Xử lý Incremental và Persistence

**1. DataFrame không phải là Persistent State**
Một Spark DataFrame chỉ tồn tại trong phạm vi của Spark session. Việc khởi động lại cluster hoặc notebook sẽ xóa sổ DataFrame đó. Do vậy, tuyệt đối không sử dụng DataFrame như một bộ nhớ để theo dõi các đơn vị Bronze ingestion nào đã được xử lý.

**2. Persistent Silver State**
Dữ liệu Weather sau khi xử lý xong sẽ được lưu trữ vật lý (persisted) tại tầng Silver trên ADLS. Bằng cách giữ lại trường `ingestion_id` trong tập dữ liệu Silver, các lần chạy pipeline trong tương lai có thể dễ dàng xác định được dữ liệu Bronze đang chờ (pending) theo công thức trừ tập hợp: `(Các ingestion ID Bronze đã commit) - (Các ingestion ID đã có ở Silver)`.

**3. Ý nghĩa của Bronze Commit (Commit Semantics)**
Một bản ghi Weather ingestion ở Bronze chỉ đủ điều kiện nạp lên Silver khi marker `_SUCCESS` tồn tại. Marker `_SUCCESS` đại diện cho việc toàn bộ đơn vị ingestion đã được commit hoàn chỉnh bao gồm cả 3 thành phần: `response.json`, `metadata.json`, và `_SUCCESS`.

**4. Khác biệt giữa Notebook và Python Module**
Databricks notebooks được sử dụng tối ưu nhất cho việc: giải thích (explanation), điều phối (orchestration), khám phá (exploration) và kiểm tra kết quả (inspecting results). Các logic transformation production tái sử dụng bắt buộc phải được chuyển vào các module Python. Cách tiếp cận này ngăn việc tầng Silver bị "phình to" thành một notebook duy nhất, đồng thời nâng cao khả năng bảo trì và testability.


## Lớp Silver — Modular Transformation và Data Quality

**1. Notebook so với Production Module**
Một Databricks notebook không nên chứa toàn bộ logic transformation cho production. Notebook được sử dụng để giải thích và điều phối workflow, trong khi các logic PySpark có thể tái sử dụng phải được triển khai trong các module Python riêng biệt.

**2. Xử lý Silver Tăng tiến (Incremental Silver Processing)**
Một Spark DataFrame mang tính tạm thời và không thể được sử dụng làm persistent processing state. Trạng thái ingestion đã xử lý được tái tạo từ dữ liệu đã lưu (persisted) ở lớp Silver. Về mặt khái niệm:
`Các ingestion ID Bronze đã commit - Các ingestion ID Silver đã lưu = Các ingestion ID đang pending`

**3. Bulk Spark Reads (Đọc hàng loạt bằng Spark)**
Việc duyệt File system có thể được sử dụng để khám phá các đường dẫn ingestion. Sau đó, Spark nên đọc các đường dẫn file đã chọn theo lô (bulk load) thay vì liên tục đọc từng ingestion đơn lẻ và union các DataFrame lại với nhau.

**4. Silver Candidate**
Quá trình transformation tạo ra một Silver Candidate trước khi bất kỳ dữ liệu nào được ghi xuống Silver. Silver Candidate đi qua các bước: Transformation → Data Quality → Validation → Silver Write.

**5. Chất lượng dữ liệu (Data Quality)**
Việc profiling Data Quality giúp đo lường các vi phạm mà không tự động sửa đổi tập dữ liệu. Mỗi metric DQ hiện tại tuân theo quy tắc: `0 = PASS`, `> 0 = FAIL`. Các bản ghi không hợp lệ sẽ không bị âm thầm điền (fill), loại bỏ (drop), hoặc khử trùng lặp (deduplicated) nếu không có một chính sách xử lý rõ ràng.

**6. Spark Driver**
Các phép Spark transformation thực thi phân tán trên các executors, trong khi các control logic của Python chạy trên Driver. Các thao tác như `collect()` sẽ tải kết quả từ các executor về lại bộ nhớ của Driver và chỉ nên được sử dụng khi tập dữ liệu kết quả được xác định chắc chắn là nhỏ.