# FastOrder Order State Machine

## 1. Mục tiêu

Tài liệu này định nghĩa vòng đời hợp lệ của `orders.order_status` trong FastOrder Simulator.

**Quan trọng:** PostgreSQL `orders` hiện là **current-state table**. Simulator thực hiện `INSERT/UPDATE` trực tiếp; hệ thống chưa có event store ghi lại mọi transition. Timestamp-based ingestion chỉ quan sát các version mà nó kịp poll, vì vậy không được gọi đây là log-based CDC hoặc khẳng định đã lưu đầy đủ mọi event chuyển trạng thái.

---

## 2. Initial State

Mọi order mới do simulator tạo bắt đầu với:

- `created` — order đã được tạo cùng payment instruction và inventory transaction tương ứng.

---

## 3. Valid Transitions

```text
created
  ├── approved
  ├── canceled
  └── unavailable

approved
  ├── processing
  └── canceled

processing
  ├── shipped
  └── canceled

shipped
  └── delivered
```

### Ý nghĩa

| State | Ý nghĩa nghiệp vụ |
|---|---|
| `created` | Đơn vừa được tạo |
| `approved` | Thanh toán/COD đã được duyệt |
| `processing` | Kho đang xử lý/đóng gói |
| `shipped` | Đã bàn giao cho đơn vị vận chuyển |
| `delivered` | Giao hàng thành công |
| `canceled` | Đơn bị hủy |
| `unavailable` | Không thể tiếp tục xử lý do điều kiện hệ thống/hàng hóa |

---

## 4. Terminal States

Simulator không advance thêm khi order đã ở một trong các trạng thái:

```text
delivered
canceled
unavailable
```

---

## 5. Data Engineering Implication

Operational ingestion dùng cursor theo `updated_at` để lấy **observed versions** của current-state table.

Ví dụ, order có thể thực tế đi qua:

```text
created -> approved -> processing -> shipped -> delivered
```

nhưng nếu các transition diễn ra giữa hai lần poll, Bronze chỉ có thể chứa những version đã được quan sát. Muốn có transition history đầy đủ cần event log hoặc log-based CDC; FastOrder hiện chưa giả lập dữ liệu lịch sử không quan sát được.
