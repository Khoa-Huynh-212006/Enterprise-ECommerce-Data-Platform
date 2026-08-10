# Orders Incremental Ingestion Pipeline

## 1. Tổng quan
Pipeline chịu trách nhiệm trích xuất dữ liệu gia tăng từ bảng `orders` (PostgreSQL) và ghi vào Local Bronze Layer dưới định dạng Parquet.

## 2. Kiến trúc và Flow
Pipeline áp dụng mô hình Dual-state management (Checkpoint và Pending Context) để đảm bảo At-least-once semantics.

**Thứ tự thực thi bắt buộc (Commit Ordering):**
1. Extract: Kéo records và next watermark từ DB.
2. Pending Context: Build và save trạng thái in-flight vào ổ đĩa.
3. Write Bronze: Ghi file Parquet (Idempotent overwrite).
4. Checkpoint: Commit thành công vị trí mới nhất.
5. Cleanup: Xóa Pending Context.

## 3. Ma trận Phục hồi sự cố (Crash Recovery Matrix)
Hệ thống có khả năng tự động khôi phục dữ liệu ở bất kỳ thời điểm ngắt điện/crash nào:

| Thời điểm Crash | Trạng thái File | Hành động Phục hồi khi Restart |
| :--- | :--- | :--- |
| Trước khi lưu Pending Context | Không có side effect | Bắt đầu lại batch mới từ Checkpoint hiện tại. |
| Sau Pending, trước khi ghi Bronze | Có Pending, chưa có Parquet | Đọc Pending Context, chạy lại batch với đúng boundaries và ghi Parquet mới. |
| Sau khi ghi Bronze, trước Checkpoint | Có Pending, có Parquet, Checkpoint cũ | Đọc Pending Context, chạy lại batch, ghi đè Parquet (Idempotent), tiến Checkpoint. |
| Sau Checkpoint, trước khi xóa Pending | Có Pending (thành rác), có Parquet, Checkpoint mới | Phát hiện Checkpoint đã tiến. Xóa bỏ file Pending rác và chạy batch tiếp theo. |

## 4. Airflow Orchestration
Tiến trình Ingestion được điều phối toàn diện thông qua Apache Airflow (DAG: `incremental_orders_dag`).
*   **Stable Run Identity:** Sử dụng `dag_run.start_date` làm `run_started_at` và làm sạch `dag_run.run_id` (Sanitize) để loại bỏ các ký tự không an toàn cho File System (như `:`, `+`). Điều này đảm bảo tính Deterministic cho việc tạo `extraction_id` và Crash Recovery.
*   **Mount Protection:** Các đường dẫn volume mount (Bronze, Checkpoint, Pending) được bảo vệ bằng cơ chế Fail-Fast, ngăn chặn việc vô tình sinh dữ liệu rác bên trong container nếu cấu hình mount bị lỗi.

## 5. Storage Backend (Next Phase)
Kiến trúc hiện tại đã hoàn thiện End-to-End MVP với Local File System (Bronze Parquet). Giai đoạn tiếp theo sẽ tích hợp Azure Data Lake Storage (ADLS Gen2) để thay thế Local Path, trong khi vẫn bảo toàn nguyên vẹn toàn bộ logic Orchestration, Extractor, Checkpoint và Crash Recovery.