# Schema Change Log

## v1 — Early draft

**Date:** 29/07/2026

Characteristics:

- Table names prefixed with `olist_*`.
- Included `is_simulated` and `simulation_id`.
- Added warehouse and inventory concepts.
- Some FK/index/timestamp gaps.

## v2 — FastOrder naming and constraints

**Date:** 29/07/2026

Changes:

- Renamed `olist_*` tables to FastOrder business table names.
- Removed simulator marker columns.
- Added warehouses and inventory relationships.
- Inventory changed to composite PK `(warehouse_id, product_id)`.
- Added NOT NULL constraints.
- Added FK and `updated_at` indexes.
- Added timestamps to inventory.
- Corrected table creation order.

## v3 — Geolocation key correction

**Date:** 29/07/2026

Incident:

- Initial load failed on geolocation with integrity violation.

Root cause:

- Composite PK `(zip_code_prefix, lat, lng)` was not a reliable key.
- Duplicate source rows and six-decimal precision could collide.

Change:

- Added surrogate identity PK `geolocation_id`.
- Removed composite PK.
- Removed geolocation deduplication from loader.
- Preserved source records.

Result:

- Initial Olist load completed successfully.

## Cập nhật bảng Warehouses cho nghiệp vụ tại Việt Nam

**Date:** 30/07/2026
### Thay đổi cấu trúc (DDL)
- **Bảng:** `warehouses`
- **Thay đổi cột:** Đổi tên `warehouse_state` thành `warehouse_region`.
- **Thay đổi kiểu dữ liệu:** Đổi từ `VARCHAR(2)` (chuẩn mã bang) thành `VARCHAR(20)` để lưu trữ tên miền (NORTH, CENTRAL, SOUTH).

### Lý do (Business Context)
- Dataset gốc Olist sử dụng mã bang 2 chữ cái (VD: SP, RJ), không phù hợp với bối cảnh địa lý của FastOrder (doanh nghiệp tại Việt Nam). 
- Thay đổi để phục vụ việc phân bổ 5 kho hàng chiến lược theo 3 miền: 
  1. Hà Nội (NORTH)
  2. Hải Phòng (NORTH)
  3. Đà Nẵng (CENTRAL)
  4. TP.HCM (SOUTH)
  5. Cần Thơ (SOUTH)

## Bổ sung cột quantity cho bảng order_items

**Date:** 30/07/2026

### Thay đổi cấu trúc (DDL)
* **Bảng:** `order_items`
* **Thay đổi:** Thêm cột `quantity INT NOT NULL DEFAULT 1`.

### Lý do (Business Context)
* Dataset gốc Olist lưu mỗi bản ghi `order_items` là 1 sản phẩm riêng biệt (quantity luôn = 1 nhưng ngầm định).
* Để phục vụ hệ thống FastOrder Faker Simulator sinh đơn hàng thực tế (khách hàng có thể mua nhiều sản phẩm cùng loại trên một dòng order line), bắt buộc phải có cột `quantity` để tính toán đúng luồng thanh toán và trừ kho.
* Việc gán `DEFAULT 1` đảm bảo tính tương thích ngược (backward compatibility) với tiến trình Initial Loader nạp từ CSV gốc.

## Triển khai và Xác thực Cột Quantity

**Date:** 30/07/2026

* **Trạng thái:** **ĐÃ TRIỂN KHAI**
* **Xác thực:** Đã bootstrap lại toàn bộ database, initial loader chạy thành công với `DEFAULT 1`. Faker Simulator đã tích hợp và sinh giá trị ngẫu nhiên (1-10) thành công cho các đơn hàng mới.

## 2026-08-05: Củng cố Incremental Source Contract cho bảng `orders`
*   **Thay đổi 1:** `ALTER TABLE orders ALTER COLUMN updated_at SET NOT NULL;`
    *   *Lý do:* Đảm bảo watermark không bao giờ bị rỗng, loại trừ rủi ro bỏ sót dữ liệu vĩnh viễn (missing records) khi trích xuất.
*   **Thay đổi 2:** `CREATE INDEX IF NOT EXISTS idx_orders_updated_at_order_id ON orders (updated_at, order_id);`
    *   *Lý do:* Tối ưu hóa hiệu năng cho các truy vấn Incremental pagination sử dụng composite watermark.