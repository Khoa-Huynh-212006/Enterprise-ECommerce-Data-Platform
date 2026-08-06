# Incremental Ingestion Strategy

Tài liệu định nghĩa chiến lược trích xuất dữ liệu tăng dần (Incremental Extraction) từ PostgreSQL (Operational Database) xuống Bronze Storage (Data Lake).

## 1. Orders Incremental Source Contract
Bảng `orders` được chọn làm mốc thử nghiệm đầu tiên với các cam kết (Invariant) sau:

*   **Extraction Method:** Timestamp-based incremental extraction.
*   **Composite Watermark:** Tiến độ trích xuất được chốt bằng cặp `(updated_at, order_id)`. Được tối ưu bởi index `idx_orders_updated_at_order_id`.
*   **Data Types & Timezone:** 
    *   `order_id`: VARCHAR(32). Tie-breaker ban đầu là `""` (chuỗi rỗng).
    *   `updated_at`: TIMESTAMP WITHOUT TIME ZONE, bắt buộc **NOT NULL**. Giá trị Checkpoint bảo toàn định dạng database-local timestamp (ISO 8601 naive, không gắn giả lập múi giờ `Z` của UTC).
*   **Mutation Invariant:** Mọi truy vấn UPDATE lên bảng `orders` bắt buộc phải cập nhật cột `updated_at`. Không hỗ trợ bắt sự kiện Hard Delete trong MVP.

## 2. Checkpoint Format
File JSON lưu vết trạng thái commit an toàn:
```json
{
  "version": 1,
  "table_name": "orders",
  "watermark": {
    "updated_at": "1970-01-01T00:00:00.000000",
    "order_id": ""
  }
}
```

## 3. Extraction Boundaries (Giới hạn trích xuất)
Để ngăn ngừa tình trạng trích xuất chạy theo "mục tiêu di động" (dữ liệu source sinh mới liên tục khi đang chạy), mỗi lượt chạy (Extraction Run) phải được "đóng băng":
*   **Lower Bound:** Lấy từ Checkpoint JSON đã commit của lần chạy trước.
*   **Upper Bound:** Query Record có `(updated_at, order_id)` lớn nhất tại thời điểm **Bắt đầu** Run.
*   **Pagination:** Chia lô (Batching) bằng `ORDER BY updated_at, order_id LIMIT :batch_size`.

## 4. Checkpoint & Manifest Architecture
*   **State Control (Checkpoint):** File JSON lưu vết watermark `updated_at` và `order_id` cuối cùng. Ghi file theo nguyên tắc Atomic (Ghi file `.tmp` -> Đổi tên đè file `.json`) để chống hỏng file khi crash.
*   **Observability (Manifest):** File JSON đi kèm mỗi Batch Parquet, lưu trữ metadata (row_count, started_at, completed_at) phục vụ audit.


## 5. Architectural Boundaries (Phân tách trách nhiệm)

Hệ thống Ingestion được thiết kế theo nguyên tắc Single Responsibility để đảm bảo tính module hóa cao nhất:

*   **Checkpoint Manager:** Chỉ quản lý trạng thái đã commit (committed position). Đảm bảo tính toàn vẹn của Watermark thông qua cơ chế Atomic Write (`os.replace`, `fsync`).
*   **Orders Extractor:** Chỉ chịu trách nhiệm giao tiếp với PostgreSQL. Nhận lower watermark từ checkpoint, tự động chụp upper watermark tại thời điểm bắt đầu chạy, đọc các batch dữ liệu trong khoảng `lower < row <= upper` và trả về kết quả (Tuyệt đối không tự ghi file storage).
*   **Bronze Writer:** Chỉ làm nhiệm vụ tiếp nhận dữ liệu dictionary từ Extractor và serialize thành định dạng Parquet an toàn tại tầng Bronze.
*   **Incremental Runner:** Đóng vai trò Orchestrator mỏng, kết nối 3 thành phần trên theo đúng trình tự và quản lý việc sinh Run Manifest.

## 6. Delivery Semantics & Retry Context
*   **PostgreSQL → Bronze (At-least-once with Idempotent Overwrite):** 
    *   Nếu một tiến trình trích xuất thất bại (ví dụ: lỗi mạng, sập nguồn) trước khi cập nhật Checkpoint, Runner sẽ đọc lại cùng một batch dữ liệu ở lần chạy tiếp theo.
    *   **Retry Context:** Runner áp dụng cơ chế *Idempotent Overwrite* bằng cách tái sử dụng `extraction_id` (ví dụ: dựa trên timestamp cửa sổ chạy hoặc Airflow Run ID). 
    *   Khi ghi xuống đĩa, `Bronze Writer` sinh file `.tmp` mới và dùng `os.replace` đè lên file Parquet rác của lần chạy lỗi trước đó. Điều này giúp ngăn chặn tình trạng phình to Data Lake (storage bloat) do các file rác bị bỏ quên.
*   **Bronze → Silver:** Lớp Silver chịu trách nhiệm hoàn toàn việc Deduplicate dữ liệu giữa các batch khác nhau bằng composite key `(order_id, updated_at)`.