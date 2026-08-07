## 07/08/2026 - Crash Recovery & State Management In Data Pipelines

**1. Ranh giới giữa Checkpoint và Pending Context**
Trong thiết kế Data Pipeline, Checkpoint đóng vai trò là "Nguồn sự thật" (Source of Truth) cho những dữ liệu đã được xác nhận an toàn. Trong khi đó, Pending Context đóng vai trò là "Bộ nhớ ngắn hạn" lưu lại ý định thực thi. Phải có cả hai mới giải quyết được bài toán Crash Recovery mà không tạo ra bản ghi nhân bản (duplicates).

**2. Sức mạnh của Idempotent Overwrite**
Cơ chế Atomic Write (`os.replace`) kết hợp với việc tái sử dụng `extraction_id` từ Pending Context giúp kiến trúc không cần phải lo lắng về việc dọn dẹp file rác nếu tiến trình sập giữa chừng. Chạy lại (Retry) đồng nghĩa với việc ghi đè lên đúng chỗ cũ một cách an toàn.

**3. Phân tích Failure Windows (Cửa sổ rủi ro)**
Không thể test hệ thống bằng cách chỉ cho nó chạy thành công. Phải dùng `unittest.mock` để chủ động cắt đứt tiến trình (raise Exceptions) tại các vị trí nhạy cảm nhất (giữa các thao tác I/O). Việc chia nhỏ thứ tự thực thi thành 5 bước độc lập cho phép cô lập lỗi và xử lý triệt để mọi trường hợp mất điện hay tắt nguồn máy chủ.

## 2026-08-07 - Stable Boundary, Fail-Fast và Crash Recovery

**1. Stable Boundary và Run Identity**
Khi phục hồi từ một sự cố, tiến trình không được phép tự do lấy cấu hình mới (như `batch_size` mới hay `run_upper_watermark` mới từ DB). Nó phải tuyệt đối trung thành với bối cảnh đã lưu trong `Pending Context`. Việc này đảm bảo tính vẹn toàn của dữ liệu, không tạo ra khoảng trống hoặc sự trùng lặp.

**2. Triết lý Fail-Fast với State Management**
Trong Data Engineering, việc âm thầm bỏ qua lỗi (như file trạng thái 0 bytes) là con đường ngắn nhất dẫn đến thảm họa. Lỗi `JSONDecodeError` khi đọc checkpoint hỏng là một rào chắn bảo vệ hệ thống khỏi việc kéo lại toàn bộ hàng chục triệu records từ đầu. Code tuyệt đối không được tự động can thiệp hay xóa state file hỏng.

**3. Idempotent Overwrite và Failure Windows**
Việc chia quá trình ingest thành 5 bước I/O rõ ràng (Extract -> Pending -> Write Bronze -> Checkpoint -> Delete Pending) cô lập hoàn toàn các "Cửa sổ rủi ro" (Failure Windows). Khi kết hợp với tính năng Idempotent Overwrite (chấp nhận ghi đè an toàn), hệ thống trở nên vững chắc trước các tình huống tắt nguồn hay ngắt kết nối đột ngột.
