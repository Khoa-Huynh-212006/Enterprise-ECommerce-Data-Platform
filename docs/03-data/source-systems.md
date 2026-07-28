# Hệ thống Nguồn (Source Systems)

## 1. Mục tiêu
Tài liệu này đặc tả chi tiết toàn bộ các nguồn dữ liệu đầu vào của hệ thống FastOrder. Để mô phỏng sát nhất với môi trường Production của một nền tảng E-commerce thực tế, hệ thống nguồn được thiết kế phân mảnh thành 3 trụ cột độc lập: Cơ sở dữ liệu quan hệ, API ngoại vi và Hệ thống lưu trữ tệp tin.

---

## 2. Sơ đồ Hệ thống Nguồn

```text
                              SOURCE SYSTEMS (Nguồn Dữ Liệu)
=========================================================================================

  [1] Operational Database      [2] External APIs             [3] File-based Systems
  (Giao dịch cốt lõi)           (Ngữ cảnh & Tham chiếu)       (Dữ liệu bán cấu trúc/Lớn)

  +----------------------+      +----------------------+      +----------------------+
  | PostgreSQL           |      | REST APIs            |      | Local Mount / SFTP   |
  |----------------------|      |----------------------|      |----------------------|
  | - Olist (Baseline)   |      | - Weather (Meteo)    |      | - Clickstream JSONL  |
  | - Faker Simulator    |      | - Holiday (Nager)    |      | - Reviews JSONL      |
  | - CDC & Incremental  |      | - Exchange Rate      |      | - Vendor Catalog CSV |
  |                      |      | - Mock Logistics API |      | - Product Snapshots  |
  +----------+-----------+      +----------+-----------+      +----------+-----------+
             |                             |                             |
             +-----------------------------+-----------------------------+
                                           |
                                           V
                            ĐẾN DATA PLATFORM (Ingestion Layer)
```
## 3. Đặc tả Chi tiết các Nguồn Dữ Liệu
### 3.1. Operational Database (Hệ thống giao dịch cốt lõi)
Đóng vai trò là trái tim của hệ thống bán hàng, lưu trữ toàn bộ thông tin về Khách hàng, Sản phẩm, Đơn hàng và Thanh toán.

Công nghệ: PostgreSQL (chạy qua Docker).

Chiến lược Dữ liệu (Hybrid Approach):

Baseline Data (Olist Dataset): Import các bảng Dimension cốt lõi từ bộ dữ liệu thật Olist (Khách hàng, Sản phẩm, Danh mục) để đảm bảo tính toàn vẹn tham chiếu (Referential Integrity) và có sẵn một lượng dữ liệu lịch sử đủ lớn.

Transaction Simulator (Python + Faker): Không dùng Faker để tạo dữ liệu ngẫu nhiên hoàn toàn. Thay vào đó, Simulator đóng vai trò tạo ra các Sự kiện (Events) liên tục dựa trên tệp khách hàng/sản phẩm có sẵn.

Mô phỏng Production:

Simulator sẽ thực hiện các lệnh INSERT (Tạo đơn) và UPDATE (Đổi trạng thái đơn, Thanh toán, Hủy đơn).

Các bản ghi luôn được cập nhật trường updated_at. Nhờ đó, luồng Ingestion của Airflow có thể thực hiện Incremental Load (chỉ lấy dữ liệu mới) dựa trên watermark, mô phỏng chính xác kỹ thuật Change Data Capture (CDC).

### 3.2. External APIs (Dữ liệu Tham chiếu & Ngữ cảnh)
Cung cấp các dữ liệu biến động theo thời gian thực hoặc theo ngày, giúp Data Platform có thêm các Dimension làm phong phú cho quá trình phân tích.

Weather API (Open-Meteo): Miễn phí, không cần key. Lấy dữ liệu thời tiết để phân tích tương quan với thời gian giao hàng.

Holiday API (Nager.Date): Cung cấp lịch nghỉ lễ của Việt Nam. Hỗ trợ xây dựng bảng dim_date để phân tích tính mùa vụ (Seasonality) và sức mua.

Exchange Rate API (Frankfurter / ExchangeRate.host): Cập nhật tỷ giá hối đoái hàng ngày, phục vụ việc tính toán biến động chi phí nhập khẩu và biên lợi nhuận trong Finance Mart.

Mock Logistics API (Custom Service): Một API giả lập (viết bằng FastAPI). Khi Data Platform truyền Order_ID lên, API này sẽ trả về trạng thái giao hàng, thời gian dự kiến (estimated_time), thời gian thực tế (actual_time), và nguyên nhân trễ hạn.

### 3.3. File-based Systems (Hệ thống lưu trữ Tệp tin)
Xử lý các loại dữ liệu sinh ra với tần suất cao (Streaming logs) hoặc dữ liệu từ các đối tác bên ngoài gửi định kỳ (Batch files), định dạng phổ biến là JSONL và CSV.

Clickstream (JSONL): File log ghi lại hành vi lướt web của người dùng (view, add_to_cart, checkout). Phục vụ phân tích Funnel và Drop-off rate.

Reviews (JSONL): Dữ liệu đánh giá của khách hàng, chứa các đoạn văn bản (text) và số sao. Phục vụ Customer Service Dashboard.

Vendor Catalog (CSV): File báo giá định kỳ từ 200 nhà cung cấp. Chứa thông tin về giá nhập (Cost) cập nhật theo tháng.

Product Snapshot (CSV): Các bản chụp trạng thái của danh mục sản phẩm lưu trữ theo thời gian để theo dõi lịch sử thay đổi (SCD Type 2).