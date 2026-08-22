## Current Focus

Giai đoạn hiện tại: **Bronze → Silver**
Tập dữ liệu đang xử lý: `weather_forecast_hourly`

### Completed

**Nghiệp vụ và mô hình hóa:**
- Xác định mục đích nghiệp vụ.
- Nắm bắt cấu trúc Bronze.
- Xác định Silver grain.
- Xác định Silver candidate schema.

**Tải dữ liệu tăng tiến (Incremental loading):**
- Khám phá các đơn vị ingestion Bronze đã commit.
- Đọc các ingestion ID đã được lưu ở tầng Silver.
- Nhận diện các đơn vị ingestion đang pending.
- Bulk load các file metadata và response đang pending.

**Chuyển đổi dữ liệu (Transformation):**
- Trích xuất ngữ cảnh ingestion.
- Làm phẳng (flatten) các mảng hourly Forecast.
- Đính kèm metadata.
- Chuẩn hóa các cột Silver.
- Chuẩn hóa timestamp sang UTC.
- Xây dựng pipeline Forecast transformation có thể tái sử dụng.

**Chất lượng dữ liệu (Data Quality):**
- Kiểm tra NULL cho các trường bắt buộc.
- Kiểm tra NULL cho các chỉ số đo lường thời tiết.
- Xác thực khoảng giá trị độ ẩm.
- Xác thực lượng mưa không âm.
- Xác thực tốc độ gió không âm.
- Xác thực trùng lặp độ chi tiết (duplicate grain).
- Chốt chặn Data Quality (Data Quality assertion).

### In Progress

- Validation cho Silver Candidate.

### Next Step

- Xác thực tính toàn vẹn của transformation (transformation completeness).
- Xác thực số lượng dòng hourly dự kiến.
- Xác thực độ bao phủ của các ingestion pending.
- Ghi dữ liệu đã validate vào Silver Delta.
- Kiểm thử cơ chế incremental replay và tính lũy đẳng (idempotency).