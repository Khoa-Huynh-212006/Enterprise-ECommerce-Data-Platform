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

## 3. Extraction Boundaries (Giới hạn trích xuất)
Để ngăn ngừa tình trạng trích xuất chạy theo "mục tiêu di động" (dữ liệu source sinh mới liên tục khi đang chạy), mỗi lượt chạy (Extraction Run) phải được "đóng băng":
*   **Lower Bound:** Lấy từ Checkpoint JSON đã commit của lần chạy trước.
*   **Upper Bound:** Query Record có `(updated_at, order_id)` lớn nhất tại thời điểm **Bắt đầu** Run.
*   **Pagination:** Chia lô (Batching) bằng `ORDER BY updated_at, order_id LIMIT :batch_size`.

## 4. Checkpoint & Manifest Architecture
*   **State Control (Checkpoint):** File JSON lưu vết watermark `updated_at` và `order_id` cuối cùng. Ghi file theo nguyên tắc Atomic (Ghi file `.tmp` -> Đổi tên đè file `.json`) để chống hỏng file khi crash.
*   **Observability (Manifest):** File JSON đi kèm mỗi Batch Parquet, lưu trữ metadata (row_count, started_at, completed_at) phục vụ audit.