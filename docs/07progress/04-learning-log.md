## 07/08/2026 - Crash Recovery & State Management In Data Pipelines

**1. Ranh giới giữa Checkpoint và Pending Context**
Trong thiết kế Data Pipeline, Checkpoint đóng vai trò là "Nguồn sự thật" (Source of Truth) cho những dữ liệu đã được xác nhận an toàn. Trong khi đó, Pending Context đóng vai trò là "Bộ nhớ ngắn hạn" lưu lại ý định thực thi. Phải có cả hai mới giải quyết được bài toán Crash Recovery mà không tạo ra bản ghi nhân bản (duplicates).

**2. Sức mạnh của Idempotent Overwrite**
Cơ chế Atomic Write (`os.replace`) kết hợp với việc tái sử dụng `extraction_id` từ Pending Context giúp kiến trúc không cần phải lo lắng về việc dọn dẹp file rác nếu tiến trình sập giữa chừng. Chạy lại (Retry) đồng nghĩa với việc ghi đè lên đúng chỗ cũ một cách an toàn.

**3. Phân tích Failure Windows (Cửa sổ rủi ro)**
Không thể test hệ thống bằng cách chỉ cho nó chạy thành công. Phải dùng `unittest.mock` để chủ động cắt đứt tiến trình (raise Exceptions) tại các vị trí nhạy cảm nhất (giữa các thao tác I/O). Việc chia nhỏ thứ tự thực thi thành 5 bước độc lập cho phép cô lập lỗi và xử lý triệt để mọi trường hợp mất điện hay tắt nguồn máy chủ.

