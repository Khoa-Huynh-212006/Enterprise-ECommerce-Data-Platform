## Current Focus

Giai đoạn phát triển hiện tại: **Bronze → Silver**
Tập dữ liệu đang xử lý: `weather_forecast_hourly`

### Completed

- Xác định mục đích nghiệp vụ.
- Nắm bắt cấu trúc dữ liệu Bronze Weather.
- Xác định Silver grain (độ chi tiết).
- Xác định Silver candidate schema.
- Hoàn thành transformation mẫu cho Forecast.
- Làm phẳng (Flatten) các mảng hourly data.
- Join với metadata.
- Chuẩn hóa ngữ nghĩa timestamp.
- Chuẩn hóa thời gian Forecast từ múi giờ `Asia/Ho_Chi_Minh` sang `UTC`.
- Vượt qua các bước kiểm tra Data Quality ban đầu trên sample ingestion.

### In Progress

Đưa bộ transformation Weather Forecast Silver lên môi trường production.
Công việc hiện tại:
- Khám phá các đơn vị ingestion Bronze Forecast đã được commit.
- Xác định những ingestion ID nào chưa được lưu (persisted) vào Silver.
- Bulk load (tải hàng loạt) các file dữ liệu phản hồi Forecast và metadata đang pending.
- Chạy transformation hiện có trên toàn bộ tập dữ liệu pending.

### Next Step

- Kiểm tra Data Quality trên toàn bộ tập dữ liệu (Full-dataset Data Quality).
- Validation dữ liệu.
- Ghi Silver Delta table.
- Xác thực tính Incremental/replay.

### Not Started

- Synapse
- dbt Gold
- Power BI