# FastOrder: Order State Machine

Tài liệu này định nghĩa Vòng đời Trạng thái (Lifecycle) của một Đơn hàng trong hệ thống FastOrder, làm cơ sở cho Data Generator (Simulator) sinh các sự kiện thay đổi trạng thái (CDC events).

## 1. Trạng thái Khởi tạo (Initial State)

Tất cả các đơn hàng được tạo ra từ event `CREATE_ORDER` bắt buộc phải mang trạng thái:
* **`created`**: Đơn hàng vừa được khởi tạo cùng với thông tin thanh toán (payment instruction).

## 2. Luồng Chuyển đổi Hợp lệ (Valid Transitions)

Simulator khi thực thi event `ADVANCE_ORDER_STATUS` chỉ được phép tịnh tiến trạng thái theo sơ đồ dưới đây (không được random nhảy cóc):

```text
created
  ├── approved    (Thanh toán được xác nhận / Đơn COD được duyệt)
  ├── canceled    (Khách hàng hủy hoặc thanh toán lỗi)
  └── unavailable (Lỗi hệ thống hoặc thất thoát kho thực tế)

approved
  ├── processing  (Kho bắt đầu đóng gói)
  └── canceled    (Hủy trước khi xuất kho)

processing
  ├── shipped     (Bàn giao cho đơn vị vận chuyển)
  └── canceled    (Hủy do lỗi đóng gói / hư hỏng phút chót)

shipped
  └── delivered   (Giao hàng thành công)
  ```

## 3. Trạng thái Kết thúc (Terminal States)
Khi đơn hàng đạt đến một trong các trạng thái sau, Simulator sẽ ngừng tác động (không sinh thêm event cho đơn hàng này):

delivered

canceled

unavailable