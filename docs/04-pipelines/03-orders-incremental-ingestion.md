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

## 4. Airflow Orchestration & End-to-End Validation
Tiến trình Ingestion được điều phối toàn diện thông qua Apache Airflow (DAG: `incremental_orders_dag`).
*   **Stable Run Identity:** Sử dụng `dag_run.start_date`, sanitize `run_id` để đảm bảo an toàn trên File System và đảm bảo tính Deterministic.
*   **Timezone-Aware Partitioning:** Thời gian `start_date` (UTC mặc định của Airflow) được convert chuẩn xác về múi giờ `Asia/Ho_Chi_Minh` trước khi đưa vào hàm tính toán `ingestion_date`. Điều này đảm bảo thư mục partition trên ADLS (ví dụ: `ingestion_date=2026-08-11`) phản ánh đúng ngày nghiệp vụ của hệ thống.
*   **E2E Validation:** Hệ thống đã chứng minh độ tin cậy qua 4 kịch bản tích hợp hoàn chỉnh (No-data rerun, Simulator delta ingestion) với ADLS Gen2.

## 5. Storage Backend (ADLS Gen2)
Toàn bộ dữ liệu Bronze Layer được lưu trữ trên Azure Data Lake Storage Gen2 dưới định dạng Parquet.
*   Quá trình upload sử dụng trực tiếp in-memory stream (`io.BytesIO`) qua `DataLakeFileClient.upload_data()` kết hợp `overwrite=True` để đáp ứng triết lý Idempotent.
*   Bảo mật: Connection/Credentials được quản lý hoàn toàn độc lập thông qua biến môi trường của Worker, tách biệt tuyệt đối khỏi mã nguồn DAG và Ingestion Runner.
