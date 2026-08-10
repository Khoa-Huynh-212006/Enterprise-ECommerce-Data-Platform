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
Tiến trình Ingestion được điều phối toàn diện thông qua Apache Airflow, đảm bảo tính Deterministic qua cơ chế Sanitize Run ID và Fail-Fast Mount Guards. Hệ thống đã vượt qua bộ kiểm định End-to-End (E2E) Operational Validation với 3 kịch bản thực tế:
1.  **Initial Incremental Load:** Xử lý thành công toàn bộ backlog dữ liệu khi chưa có Checkpoint (bắt đầu từ mốc `1970-01-01`).
2.  **Immediate Rerun (No New Data):** Nhận diện chính xác trạng thái không có dữ liệu mới, không sinh thêm file Parquet rác, không làm phình Data Lake.
3.  **New Source Delta:** Nắm bắt chuẩn xác các thay đổi mới từ DB (Insert/Update) dựa trên composite watermark, tịnh tiến Checkpoint an toàn và chỉ ghi đúng dữ liệu Delta.

## 5. Storage Backend (Next Phase)
Kiến trúc hiện tại đã hoàn thiện End-to-End MVP với Local File System (Bronze Parquet). Giai đoạn tiếp theo sẽ tích hợp Azure Data Lake Storage (ADLS Gen2) để thay thế Local Path, trong khi vẫn bảo toàn nguyên vẹn toàn bộ logic Orchestration, Extractor, Checkpoint và Crash Recovery.