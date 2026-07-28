# Các Thực thể Kinh doanh (Business Entities)

## 1. Mục tiêu
Tài liệu này định danh và phân loại toàn bộ các thực thể dữ liệu (Business Entities) cấu thành nên hệ sinh thái của FastOrder. Việc xác định rõ nguồn gốc (Source) và cơ chế thu thập (Ingestion Method) của từng thực thể là cơ sở cực kỳ quan trọng để thiết kế các luồng ống dẫn dữ liệu (Data Pipelines) và mô hình hóa dữ liệu (Data Modeling) tại tầng Data Warehouse.

## 2. Danh mục Thực thể và Nguồn cấp

Hệ thống phân chia các thực thể thành 3 nhóm cốt lõi: Giao dịch (Core Transactions), Hành vi & Đối tác (Behavior & Partners), và Ngữ cảnh (Context).

| Nhóm | Thực thể (Entity) | Nguồn cấp (Source) | Mô tả (Description) | Cách thức thu thập (Ingestion) |
| :--- | :--- | :--- | :--- | :--- |
| **Giao dịch** | **Customers** | Olist + Faker Simulator | Thông tin khách hàng. | Lấy dữ liệu của Olist làm gốc + CDC & Incremental. |
| | **Orders / Order Items** | Olist + Faker Simulator | Thông tin đơn hàng và chi tiết từng món trong đơn. | Lấy dữ liệu của Olist làm gốc + CDC & Incremental. |
| | **Products** | Olist + Faker Simulator | Thông tin danh mục sản phẩm. | Full Load / Incremental. |
| | **Sellers** | Olist + Faker Simulator | Thông tin nhà bán hàng / cung cấp. | Lấy dữ liệu của Olist làm gốc + CDC & Incremental. |
| | **Payments** | Olist + Faker Simulator | Giao dịch thanh toán của đơn hàng. | Lấy dữ liệu của Olist làm gốc + CDC & Incremental. |
| | **Inventory** | Custom | Lịch sử xuất/nhập, số lượng tồn kho tại các trạm. | CDC / Incremental. |
| **Hành vi & Đối tác** | **Clickstream** | File Storage (JSONL) | Dữ liệu hành vi: lượt xem, click, thêm vào giỏ hàng. | Batch Processing (Quét thư mục). |
| | **Reviews** | File Storage (JSONL) | Điểm đánh giá, số sao, bình luận của khách hàng. | Batch Processing. |
| | **Vendor Catalog** | File Storage (CSV) | Báo giá nhập khẩu mới cập nhật từ đối tác. | Batch Processing (Weekly). |
| **Ngữ cảnh** | **Weather** | External API (Open-Meteo) | Tình trạng thời tiết theo ngày / địa điểm kho hàng. | Daily API Call (Airflow). |
| | **Exchange Rates** | External API (Frankfurter) | Tỷ giá biến động ngoại tệ (USD/CNY to VND). | Daily API Call. |
| | **Shipping Status** | Mock API | Lộ trình và trạng thái vận chuyển của từng đơn hàng. | Event-triggered API Call. |
| | **Holidays** | External API (Nager.Date) | Danh sách lịch nghỉ lễ quốc gia. | Yearly API Call. |

## 3. Ứng dụng trong Mô hình Dữ liệu (Data Modeling)

Tại tầng Gold (Data Warehouse), các thực thể trên sẽ được `dbt` nhào nặn và kết nối với nhau theo cấu trúc **Star Schema** để phục vụ phân tích:

*   **Dimension Tables (Bảng chiều - Phân tích theo ngữ cảnh):** Được xây dựng từ `Customers`, `Products`, `Sellers`, `Weather`, `Holidays`.
*   **Fact Tables (Bảng sự kiện - Chứa các chỉ số đo lường):** Được xây dựng từ `Orders`, `Order Items`, `Payments`, `Inventory`, `Clickstream`, `Reviews`, `Shipping Status`.