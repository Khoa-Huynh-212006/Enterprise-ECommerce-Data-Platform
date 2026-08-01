# Simulator Data Quality Validation Rules

Bộ kiểm thử `validate_simulator.py` được thiết kế theo hướng Negative Testing. Mọi câu lệnh SQL đều nhắm tới việc tìm ra dữ liệu lỗi. Expected result cho tất cả các query là `0 rows`.

## Nhóm 1: Ràng buộc Toàn vẹn & Tài chính (Completeness & Aggregate)
*   **Rule 1:** Mọi đơn hàng simulator phải có ít nhất 1 `order_item`.
*   **Rule 2:** Mọi đơn hàng simulator phải có đúng 1 `order_payment`.
*   **Rule 3:** Tổng giá trị thanh toán phải khớp tuyệt đối với giá trị hàng hóa: `payment_value = SUM(price * quantity + freight_value)`. (Cho phép sai số làm tròn 0.01).

## Nhóm 2 & 3: Nhất quán Không gian & Giới hạn Vật lý (Consistency & Physical Boundaries)
*   **Rule 4:** `orders.warehouse_id` phải trùng khớp tuyệt đối với `warehouse_id` của tất cả các `order_items` bên trong nó (chống xuất hàng chéo kho).
*   **Rule 5:** Số lượng (`quantity`) trong `order_items` phải luôn > 0.
*   **Rule 6:** Số lượng tồn kho (`inventory.quantity_available`) không bao giờ được phép nhỏ hơn 0 (Chống Overselling).
*   **Rule 7:** `source_system` chỉ được phép chứa các giá trị chuẩn (`olist_seed`, `simulator`).

## Nhóm 4: Dòng thời gian & Trạng thái (Temporal & State Logic)
*   **Rule 8:** `order_status` không được phép `NULL` và phải nằm trong tập hợp trạng thái hợp lệ đã định nghĩa.
*   **Rule 9:** Ma trận Approved - `approved`, `processing`, `shipped`, `delivered` bắt buộc phải có `order_approved_at`. Các trạng thái còn lại bắt buộc `NULL`.
*   **Rule 10:** Ma trận Carrier - `shipped`, `delivered` bắt buộc phải có `order_delivered_carrier_date`. Các trạng thái còn lại bắt buộc `NULL`.
*   **Rule 11:** Ma trận Customer - Chỉ `delivered` mới có `order_delivered_customer_date`. Các trạng thái còn lại bắt buộc `NULL`.
*   **Rule 12:** Định luật nhân quả thời gian (Time-series Physicality): 
    * `order_purchase_timestamp` <= `order_approved_at` <= `order_delivered_carrier_date` <= `order_delivered_customer_date`.
    * Technical Timestamp (`updated_at`) không được nhỏ hơn thời gian tạo đơn.