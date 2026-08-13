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