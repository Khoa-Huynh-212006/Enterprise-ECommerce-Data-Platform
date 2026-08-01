# Order State Machine (MVP)

Tài liệu này định nghĩa vòng đời hợp lệ của một đơn hàng sinh ra từ Simulator (tính đến 31/07/2026).

## 1. Luồng tịnh tiến trạng thái (State Transitions)
Nhằm đơn giản hóa hệ thống MVP và tránh phình to scope sang quản lý hoàn tiền (Refund), việc hủy đơn chỉ được phép diễn ra ở trạng thái khởi tạo.

*   `created`
    *   -> `approved` (90%)
    *   -> `canceled` (8%) - Kèm logic hoàn trả tồn kho (Restock)
    *   -> `unavailable` (2%) - Kèm logic hoàn trả tồn kho (Restock)
*   `approved` -> `processing` (100%)
*   `processing` -> `shipped` (100%)
*   `shipped` -> `delivered` (100%)

## 2. Ma trận Dấu vết Thời gian (Timestamp Matrix)
Mọi trạng thái khi cập nhật bắt buộc phải tuân thủ sự hiện diện của các mốc thời gian (Business Timestamps) tương ứng:

| Trạng thái (Status) | approved_at | carrier_date | customer_date |
| :--- | :---: | :---: | :---: |
| `created` | NULL | NULL | NULL |
| `canceled` / `unavailable` | NULL | NULL | NULL |
| `approved` / `processing`| **CÓ** | NULL | NULL |
| `shipped` | **CÓ** | **CÓ** | NULL |
| `delivered` | **CÓ** | **CÓ** | **CÓ** |