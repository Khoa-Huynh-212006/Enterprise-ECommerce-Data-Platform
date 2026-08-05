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

## 3. Simulator Operational Workflow (Milestone: 2026-08-01)

Hệ thống Simulator Runner điều phối luồng trạng thái hoạt động với các đặc tính sau:
*   **Chế độ chạy:** Hỗ trợ chạy hữu hạn (Bounded mode) hoặc liên tục (Continuous mode).
*   **Khởi tạo:** Tạo ngẫu nhiên 0–3 orders mỗi cycle.
*   **Luân chuyển:** Gọi hàm `order_status_updater` mỗi cycle để tịnh tiến trạng thái đơn hàng.
*   **Observability:** Tổng hợp cycle/session metrics.
*   **An toàn:** Hỗ trợ graceful shutdown bằng Ctrl+C (dừng lịch sự, không treo Database).
*   **Kiểm định:** Chạy independent validator ngay sau khi kết thúc chuỗi mô phỏng.
*   **Trạng thái kiểm thử:** Smoke test continuous mode thành công. 12/12 validation rules PASS trên 15 simulated orders.