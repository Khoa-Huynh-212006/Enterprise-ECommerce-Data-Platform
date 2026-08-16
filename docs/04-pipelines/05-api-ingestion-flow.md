# Weather API Ingestion Flow

Hệ thống cung cấp dữ liệu thời tiết cho FastOrder (Open-Meteo) được thiết kế với hai luồng chính, tuân thủ nguyên tắc lũy đẳng (Idempotency) và Sidecar Pattern tại lớp Bronze.

## 1. Forecast Flow (Incremental)
- **Mục đích:** Lấy dự báo 48h tới phục vụ cảnh báo vận hành logistics (giao hàng trễ do mưa/bão).
- **Tần suất:** Chạy định kỳ (Incremental).
- **Đơn vị xử lý:** Lặp qua 5 kho hàng. Mỗi kho hàng tạo ra một UUID5 Snapshot độc lập.

## 2. Historical Forecast Flow (Bootstrap/Backfill)
- **Mục đích:** Nạp dữ liệu thời tiết quá khứ phục vụ phân tích tương quan giữa thời tiết và lịch sử đơn hàng.
- **Chiến lược:** Backfill 90 ngày quá khứ.
- **Windowing:** Cắt nhỏ 90 ngày thành các khoảng 30 ngày (30-day deterministic windows) để an toàn khi retry và giảm tải API payload.

## State & Recovery Mechanism
- Không sử dụng global state manifest.
- Sử dụng UUID5 (băm từ `api_type`, `warehouse_id`, `run_id`) để cố định Bronze Path.
- Tuân thủ quy trình Commit 3 bước: Ghi `response.json` ➔ Ghi `metadata.json` ➔ Ghi `_SUCCESS`. Bất kỳ Crash nào trước `_SUCCESS` đều được Airflow retry an toàn bằng cách ghi đè cục bộ.