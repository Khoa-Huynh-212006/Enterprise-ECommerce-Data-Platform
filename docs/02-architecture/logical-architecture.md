# Logical Architecture (Kiến trúc Logic)

## Mục tiêu
Tài liệu này xác định các khối chức năng cốt lõi của Data Platform cho hệ thống FastOrder. Mục tiêu là mô tả **hệ thống làm gì** và **trách nhiệm của từng vùng (Zone)**, hoàn toàn không phụ thuộc vào công nghệ được sử dụng (Spark, Azure, v.v.).

---

## Các khối chức năng

### 1. Source Systems (Hệ thống Nguồn)
* **Trách nhiệm:** Nơi sinh ra và lưu trữ dữ liệu gốc của doanh nghiệp. 
* **Đặc điểm:** Dữ liệu ở đây có thể thay đổi (Insert/Update/Delete) liên tục và ở nhiều định dạng khác nhau (Relational, API, File).

### 2. Data Ingestion (Thu thập Dữ liệu)
* **Trách nhiệm:** Kết nối đến các Source Systems để trích xuất dữ liệu và tải (Load) vào hệ thống Data Platform.
* **Đặc điểm:** Không làm thay đổi logic nghiệp vụ của dữ liệu, chỉ chịu trách nhiệm vận chuyển an toàn, đúng hạn và ghi nhận trạng thái luồng tải.

### 3. Raw Data Zone (Vùng Dữ liệu Thô)
* **Trách nhiệm:** Lưu trữ nguyên bản dữ liệu được kéo về từ hệ thống nguồn mà không qua bất kỳ chỉnh sửa nào.
* **Đặc điểm:** Hoạt động như một bản ghi lịch sử bất biến (Immutable). Đảm bảo khả năng phục hồi (Recovery) và chạy lại (Replay) toàn bộ luồng xử lý phía sau nếu có lỗi xảy ra.

### 4. Data Processing (Xử lý Dữ liệu)
* **Trách nhiệm:** Thực hiện các tác vụ tính toán để di chuyển dữ liệu giữa các Zone. Bao gồm làm sạch (Cleansing), chuẩn hóa (Standardization), kết hợp (Joining), và áp dụng các quy tắc nghiệp vụ (Business Logic).
* **Đặc điểm:** Là "động cơ" của toàn bộ hệ thống, hoạt động theo các lịch trình (Batch) hoặc luồng sự kiện.

### 5. Curated Data Zone (Vùng Dữ liệu Tinh chế)
* **Trách nhiệm:** Lưu trữ dữ liệu đã được làm sạch, chuẩn hóa và mô hình hóa thành các thực thể nghiệp vụ (Khách hàng, Sản phẩm, Đơn hàng...).
* **Đặc điểm:** Đóng vai trò là "Single Source of Truth" (Nguồn sự thật duy nhất) cho toàn doanh nghiệp. Dữ liệu ở đây có độ tin cậy cao và sẵn sàng để phân tích.

### 6. Analytics Storage (Lưu trữ Phân tích)
* **Trách nhiệm:** Tổ chức dữ liệu theo dạng Data Marts để phục vụ tối ưu cho các truy vấn phân tích của từng phòng ban (Sales, Marketing, Logistics...).
* **Đặc điểm:** Thường được thiết kế theo mô hình chiều (Star Schema/Snowflake) để tối ưu hóa hiệu suất đọc và tổng hợp (Aggregation).

### 7. Data Consumption (Khai thác Dữ liệu)
* **Trách nhiệm:** Lớp giao tiếp cuối cùng cung cấp giao diện để End-users (CEO, Managers) tương tác với dữ liệu.
* **Đặc điểm:** Bao gồm các Dashboards, báo cáo tự động, hoặc trích xuất dữ liệu ad-hoc.

---

## Luồng dữ liệu tổng quát
Dữ liệu di chuyển xuyên suốt hệ thống theo một chiều tuyến tính để đảm bảo tính nhất quán và dễ dàng truy vết (Data Lineage):

`Source Systems` ➔ `Data Ingestion` ➔ `Raw Data Zone` ➔ `Data Processing` ➔ `Curated Data Zone` ➔ `Data Processing` ➔ `Analytics Storage` ➔ `Data Consumption`

*(Workflow Orchestration sẽ nằm bên ngoài luồng này, đóng vai trò điều phối và giám sát sự kiện để kích hoạt các bước trên).*

---

## Nguyên tắc thiết kế (Design Principles)
1. **Single Responsibility:** Mỗi Zone (Vùng) chỉ đảm nhận một trách nhiệm duy nhất (lưu trữ thô, lưu trữ tinh chế, hoặc phục vụ phân tích).
2. **Immutability ở Raw Zone:** Dữ liệu đã ghi vào Raw Zone sẽ không bao giờ bị ghi đè hay xóa bỏ, đảm bảo khả năng audit và replay.
3. **Decoupling Storage and Compute:** Tách biệt rõ ràng nơi lưu trữ dữ liệu (Storage Zones) và khối tính toán/biến đổi dữ liệu (Data Processing).