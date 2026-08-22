# Pipeline Silver Weather Forecast

## Mục đích (Purpose)
Chuyển đổi (transform) các đơn vị ingestion Open-Meteo Forecast đã được commit ở lớp Bronze thành tập dữ liệu Silver Delta `weather_forecast_hourly` theo cơ chế tăng tiến (incremental).

## Luồng xử lý (Flow)
Bronze đã commit (Committed Bronze)
→ Phát hiện dữ liệu Silver đã xử lý (Processed Silver Detection)
→ Lọc ra dữ liệu chờ (Pending Selection)
→ Tải hàng loạt (Bulk Load)
→ Transform dữ liệu (Transformation)
→ Silver Candidate
→ Chất lượng dữ liệu (Data Quality)
→ Xác thực (Validation)
→ Ghi Delta (Delta Write)
→ Xác minh việc ghi (Read-back Verification)

## Quy tắc Tăng tiến (Incremental Rule)
`Pending = Committed Bronze - Processed Silver`

## Kết quả Thành công (Successful Outcomes)

### SUCCESS
Các đơn vị ingestion pending mới đã được chuyển đổi, validate và lưu trữ thành công xuống Data Lake.

### NO_OP
Không phát sinh thêm đơn vị ingestion pending nào mới, dữ liệu tại tầng Silver đã được cập nhật mới nhất.

## Điều kiện Thất bại (Failure Conditions)
Pipeline sẽ tự động fail (báo lỗi) khi gặp các tình huống:
- Lỗi truy cập bộ nhớ lưu trữ (storage access fails).
- Thất bại tại chốt chặn Data Quality.
- Thất bại tại bước xác thực tính toàn vẹn (transformation validation fails).
- Thất bại khi xác minh lại (read-back) sau quá trình ghi Silver.

*Lưu ý: Dữ liệu lỗi sẽ không bao giờ bị âm thầm lưu trữ xuống tầng Silver.*