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


## Ghi chú học tập - Weather Forecast Silver

**1. Silver Candidate**
Silver Candidate không phải là một lớp lưu trữ riêng biệt. Nó chỉ là một Spark DataFrame tạm thời, đã mang cấu trúc chuẩn của Silver schema nhưng chưa được lưu trữ (persisted) vật lý.
Luồng dữ liệu: Bronze → Transformation → Silver Candidate → Data Quality → Validation → Silver. Chỉ dữ liệu đã được validate hoàn toàn mới trở thành dữ liệu Silver lưu trữ vĩnh viễn.

**2. Spark DataFrame vs Persistent Storage**
Spark DataFrames có tính chất tạm thời. Khởi động lại Spark session hoặc cluster sẽ xóa toàn bộ biến Python và trạng thái của DataFrame. Trạng thái cần lưu trữ bền vững phải được ghi ra bên ngoài, ví dụ: dữ liệu Bronze trên ADLS, bảng Silver Delta trên ADLS. Do đó, logic incremental không được phép phụ thuộc vào việc DataFrame có tồn tại giữa các lần chạy hay không.

**3. Spark Driver và `collect()`**
Spark thực thi tính toán phân tán thông qua các executors. Driver có vai trò điều phối quá trình thực thi và chạy các control logic Python. Hàm `collect()` có nhiệm vụ tải toàn bộ kết quả tính toán phân tán của Spark về bộ nhớ của Driver. Nó chỉ an toàn khi kết quả trả về chắc chắn là một tập dữ liệu nhỏ. Trong Forecast Silver pipeline, `collect()` chỉ được dùng cho các tập dữ liệu nhỏ như danh sách `distinct(ingestion_id)`.

**4. Azure SDK Paths vs Spark Paths**
Các hàm discovery của thư viện ADLS SDK sẽ trả về đường dẫn tương đối tính từ container, ví dụ: `weather/open_meteo/forecast/...`. Ngược lại, Spark yêu cầu một URI lưu trữ đầy đủ (fully qualified storage URI): `abfss://bronze@<storage-account>.dfs.core.windows.net/...`. Hệ quả: Azure SDK paths dùng để khám phá thư mục; ABFSS paths dùng cho quá trình Spark đọc dữ liệu.

**5. Tải hàng loạt (Bulk Loading)**
Đường dẫn filesystem có thể được khám phá thông qua vòng lặp Python. Nhưng bản thân dữ liệu cần được truyền vào Spark dưới dạng một danh sách các đường dẫn để đọc hàng loạt (bulk load) cùng một lúc, thay vì đọc từng đơn vị ingestion rồi union (nối) chúng lại.

**6. Delta Lake**
Delta Lake không phải là một database server. Về mặt khái niệm: `Delta = File dữ liệu Parquet + Transaction log`. Thành phần `_delta_log` giúp theo dõi các phiên bản bảng (table versions) và các thay đổi đã được commit. Delta được chọn cho tầng Silver vì lớp này cần một định dạng bảng có tính lưu trữ bền vững (persistent), giao dịch (transactional), và hỗ trợ các truy vấn tăng tiến (incremental).

**7. Data Quality vs Validation**
- Data Quality trả lời câu hỏi: **"Các giá trị dữ liệu có hợp lệ không?"** (Ví dụ: trường NULL, độ ẩm sai, lượng mưa âm, trùng lặp độ chi tiết).
- Validation trả lời câu hỏi: **"Quá trình transformation có xử lý đẩy đủ lượng dữ liệu kỳ vọng không?"** (Ví dụ: thiếu ingestion, thiếu số giờ dự báo 48h, có ingestion ID lạ, sai lệch số dòng thực tế so với kỳ vọng).
Cả hai điều kiện đều phải PASS trước khi ghi xuống Silver.

**8. Data Quality Fail-Fast**
Pipeline hiện tại không ép buộc chạy thành công bằng cách âm thầm làm sạch dữ liệu không hợp lệ. Khi phát hiện dữ liệu lỗi:
- Data Quality thất bại.
- Hủy thao tác ghi xuống Silver.
- Các ingestion bị lỗi vẫn giữ nguyên trạng thái chưa xử lý (unprocessed).
- Developer có thể kiểm tra vấn đề và đề xuất một chính sách xử lý tường minh.

**9. Chạy Tăng tiến Rỗng (NO_OP Incremental Runs)**
Việc không có ingestion chờ xử lý (pending) không phải là lỗi. Khi `Các ingestion ID Bronze đã commit = Các ingestion ID đã xử lý ở Silver`, pipeline sẽ trả về `NO_OP` và bỏ qua toàn bộ phần transformation hay ghi Silver. Điều này chứng minh các đơn vị ingestion đã xử lý trước đó không bao giờ bị xử lý lại vô ích.

**10. Notebook vs Python Module**
Logic transformation dành cho môi trường production phải nằm ở các module Python tái sử dụng. Các development notebooks chỉ nên dùng để: tài liệu hóa, khám phá dữ liệu, debug, và kiểm tra kết quả trung gian. Một runner notebook nhỏ (thin runner) sẽ được dùng riêng cho việc thực thi End-to-End. Việc này giúp luồng làm việc dễ hiểu và không trói buộc quá trình chạy production vào state tĩnh của notebook cell.


## Weather History Silver — Learning Notes

### Business Grain khác Ingestion Grain

Bronze được tổ chức theo ingestion.

Silver Historical được tổ chức theo business grain:

`warehouse_id + weather_time`

Một ingestion có thể chứa hàng trăm hourly records và nhiều ingestion khác nhau có thể
cùng mô tả một historical hour.

Do đó ingestion identity và business identity phải được xem là hai khái niệm khác nhau.

### Overlapping Backfill Windows

Historical backfill có thể tạo ra các ingestion window bị overlap.

Một overlap thực tế được phát hiện trong FastOrder:

`2026-08-10 → 2026-08-12`

nằm bên trong:

`2026-07-17 → 2026-08-15`

Ba ngày overlap tạo ra:

`3 × 24 = 72`

duplicate historical business keys.

Đây không phải lỗi Transformation mà là đặc tính có thể xuất hiện trong ingestion history.

### Raw Candidate và Resolved Candidate

Historical pipeline sử dụng hai trạng thái candidate:

`Raw Historical Candidate`

chứa toàn bộ dữ liệu của từng ingestion trước reconciliation.

`Resolved Historical Candidate`

là canonical dataset sau khi overlapping business keys đã được xử lý.

Điều này cho phép pipeline validation từng ingestion trước khi loại duplicate khỏi
canonical dataset.

### Validation phải diễn ra trước Reconciliation

Mỗi ingestion phải được chứng minh là đầy đủ trước khi overlap records bị loại.

Nếu reconciliation được thực hiện trước ingestion validation, một ingestion overlap có thể
mất rows và tạo false validation failure.

Do đó thứ tự đúng:

`Raw Candidate`
→ `Ingestion Validation`
→ `Overlap Reconciliation`

### Expected và Actual phải độc lập

Expected validation contract phải đến từ Bronze metadata:

`start_date + end_date`

Actual result phải đến từ transformed DataFrame.

Không được suy expected row count từ chính actual output vì điều đó có thể che giấu
missing ingestion.

### Canonical Data và Processing State

Canonical Silver không phải nơi phù hợp để suy ra toàn bộ processing state.

Một ingestion có thể đã được xử lý nhưng không còn row đại diện trong canonical dataset
sau reconciliation.

Do đó Historical tách:

`weather_history_hourly`
= canonical business data

`weather_history_processed_ingestions`
= processing state

### Delta MERGE

Historical sử dụng MERGE vì business grain phải unique xuyên qua nhiều incremental run.

MERGE key:

`warehouse_id + weather_time`

Điều này khác Forecast, nơi `ingestion_id` là một phần của grain và nhiều Forecast
snapshot cho cùng forecast hour là hợp lệ.

### Safe Commit Ordering

Processing control chỉ được ghi sau canonical Silver write thành công.

An toàn:

`Canonical SUCCESS`
→ `Control SUCCESS`

Nếu control write fail, batch có thể retry vì MERGE idempotent theo business key.

Không an toàn:

`Control SUCCESS`
→ `Canonical FAIL`

vì ingestion có thể bị đánh dấu processed dù business data chưa được persist.

### NO_OP

Không có pending ingestion là một trạng thái thành công.

Historical pipeline sử dụng:

`Committed Bronze IDs - Processed Control IDs = Pending IDs`

Nếu tập Pending rỗng:

`NO_OP`

Pipeline không tiếp tục load hoặc transform dữ liệu.


## PostgreSQL Generic Ingestion — Learning Notes

### Tại sao Timestamp-only Watermark không đủ?

Nếu chỉ sử dụng cột timestamp, các bản ghi có cùng thời gian cập nhật nằm tại
biên của batch sẽ bị duplicate hoặc bị bỏ sót do thiếu thứ tự phân giải.

### Primary Key Tie-breaker

Kết hợp `timestamp` và `primary_key` tạo ra một strict ordering cursor.

Ngay cả khi có nhiều bản ghi phát sinh trong cùng một mili-giây, PK tie-breaker
đảm bảo mỗi bản ghi có một vị trí duy nhất trong chuỗi pagination.

### Customers Proof-of-Generality

Bảng Customers chứa ~99k rows chia sẻ chính xác cùng một mốc timestamp.

Nhờ cơ chế cursor `(updated_at, customer_id)`, framework vẫn pagination mượt mà
qua toàn bộ khối dữ liệu đồng thời này mà không bị lặp hay sót bất kỳ dòng nào.

### Deterministic Extraction ID

ID của mỗi extraction batch được tạo deterministic từ cấu hình bảng và state.

Điều này đảm bảo đường dẫn lưu trữ Bronze không thay đổi giữa các lần Airflow retry,
đảm bảo tính lũy đẳng (idempotency) thông qua an toàn ghi đè.

### Checkpoint và Pending Recovery

Sử dụng flat checkpoint format và crash-recovery protocol giúp hệ thống cô lập.

Trạng thái PENDING cho phép framework nhận diện chính xác điểm crash và
resume đúng state mà không cần truy vấn lại hệ thống nguồn từ đầu.