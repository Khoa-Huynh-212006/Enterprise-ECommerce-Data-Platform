# Tổng quan Bối cảnh Nghiệp vụ (Business Context)

## 1. Giới thiệu Doanh nghiệp: FastOrder
* **Lĩnh vực:** Thương mại điện tử.
* **Mô hình hoạt động:** Marketplace B2C. Khách hàng mua sắm trên sàn, Seller đăng bán sản phẩm. FastOrder đóng vai trò trung gian và thu hoa hồng.
* **Lý do chọn mô hình này để phân tích:**
  * Có nhiều luồng business phức tạp hơn B2C thuần túy.
  * Xuất hiện thêm các Dimension phức tạp (như Seller) và các Fact quan trọng (như Commission).
  * Phát sinh nhiều chỉ số KPIs thú vị, đáp ứng tốt cho bài toán thiết kế Data Platform.

## 2. Business Scope
* **Khách hàng:** Cá nhân (18-60 tuổi) mua sắm trực tuyến trên toàn Việt Nam.
* **Sản phẩm:** Hàng hóa vật lý thuộc các ngành hàng điện tử, thời trang, gia dụng, mỹ phẩm...
* **Quy mô vận hành:**
  * 5 kho hàng trên toàn quốc.
  * 200 nhà cung cấp / Seller.
  * Lưu lượng xử lý: **50.000 đơn hàng/ngày**.

## 3. Quy trình Nghiệp vụ 
---

### Quy trình Nghiệp vụ Tiêu chuẩn (Happy Path)

Đây là luồng đi lý tưởng khi một khách hàng thực hiện mua sắm thành công mà không gặp bất kỳ trở ngại nào.

`Browse (Xem sản phẩm)` ➔ `Add to Cart (Thêm giỏ hàng)` ➔ `Checkout (Tạo đơn nháp)` ➔ `Payment (Thanh toán)` ➔ `Order Created (Xác nhận đơn)` ➔ `Inventory Reserved (Giữ kho)` ➔ `Packing (Đóng gói)` ➔ `Shipping (Giao cho ĐVVC)` ➔ `Delivered (Giao thành công)` ➔ `Review (Đánh giá)`.

---

### Quy trình Ngoại lệ (Exception Paths)

Trong thực tế hệ thống E-commerce, dữ liệu sẽ ghi nhận rất nhiều luồng ngoại lệ. Các Pipeline cần phải bắt được những trạng thái này để tính toán chính xác tỷ lệ rớt đơn (Drop-off rate), tỷ lệ hoàn hàng (Return rate) và tỷ lệ hết hàng (Stock-out rate).

* **Ngoại lệ 1: Rớt thanh toán (Payment Failed)**
  * `Checkout` ➔ `Payment Failed` ➔ `Retry Payment` (Thử lại) ➔ Thành công (quay về Happy Path) hoặc `Cancelled` (Hủy đơn do quá hạn thanh toán).
* **Ngoại lệ 2: Hết hàng hệ thống (Stock-out / Overselling)**
  * `Order Created` ➔ `Inventory Reservation Failed` (Kho thực tế không đủ) ➔ `Cancelled` (Hệ thống tự động hủy và hoàn tiền - Refund).
* **Ngoại lệ 3: Khách hàng chủ động hủy (Customer Canceled)**
  * Khách hàng có thể Hủy đơn ở các bước: `Order Created`, `Inventory Reserved`, hoặc `Packing`. 
  * Nếu đã qua bước `Shipping`, khách không thể hủy trên app mà phải Từ chối nhận hàng (Ngoại lệ 4).
* **Ngoại lệ 4: Giao hàng thất bại (Delivery Failed / RTS)**
  * `Shipping` ➔ `Delivery Attempted` (Giao không được do sai địa chỉ / không nghe máy) ➔ `Returned to Sender (RTS)` (Hoàn trả về kho).
* **Ngoại lệ 5: Hoàn trả sau khi nhận (Customer Return)**
  * `Delivered` ➔ `Return Requested` (Khách yêu cầu trả hàng do lỗi) ➔ `Returned` (Nhập lại kho) ➔ `Refunded` (Hoàn tiền).

---

### Trạng thái Đơn hàng (Order State Machine)

Bảng dưới đây liệt kê các trạng thái lưu trong cơ sở dữ liệu (Source Table: `Orders`) giúp theo dõi Lifecycle của một đơn hàng. Bảng này sẽ map trực tiếp thành `dim_order_status`.

| Mã Trạng Thái (Status) | Mô tả chi tiết (Description) | Điểm kết thúc (Terminal State)? |
| :--- | :--- | :---: |
| `PENDING_PAYMENT` | Đơn hàng đã được tạo, chờ xác nhận thanh toán từ Cổng thanh toán. | ❌ |
| `PROCESSING` | Đã thanh toán, hệ thống đang giữ kho và tiến hành đóng gói. | ❌ |
| `SHIPPED` | Đơn hàng đã được bàn giao cho Đơn vị vận chuyển (ĐVVC). | ❌ |
| `DELIVERED` | Khách hàng đã nhận được hàng thành công. | ✅ |
| `CANCELLED` | Đơn hàng bị hủy (do khách hàng, do hết hàng, hoặc lỗi thanh toán). | ✅ |
| `RETURNED` | Khách hàng đã trả lại hàng thành công và kho đã nhận lại. | ✅ |
| `REFUNDED` | Đã hoàn tiền cho khách hàng (thường đi kèm Cancelled hoặc Returned). | ✅ |

---

**Các biến số ảnh hưởng đến hiệu suất hệ thống cần phân tích:**
* Yếu tố thời tiết và Thời gian (Ngày lễ, sự kiện).
* Trạng thái Giao hàng/Đóng gói.
* Phương thức thanh toán.
* Chi phí Xuất/Nhập khẩu.

### Từ điển sự kiện

---

## 4. Hiện trạng Hệ thống Nguồn (Source Systems)
Dữ liệu hiện đang bị phân mảnh ở 3 hệ thống chính:
1. **Operational Database:** RDBMS chứa dữ liệu cốt lõi (Orders, Customers, Inventory). mô phỏng hệ thống giao dịch. Hàng triệu bản ghi có thể sinh bằng script hoặc dataset thực tế.
2. **External APIs:** bổ sung ngữ cảnh nghiệp vụ (thời tiết, ngày lễ, tỷ giá, trạng thái vận chuyển...). Dữ liệu bổ sung được ingest định kỳ và lưu lại trong Data Lake
3. **File-based Source Systems:** Dữ liệu bán cấu trúc và dữ liệu lớn (clickstream, đánh giá khách hàng, catalog nhà cung cấp...). Dữ liệu lịch sử, log, review, catalog ở định dạng CSV/JSON/Parquet.

API không phải để cung cấp dữ liệu lớn, mà để cung cấp dữ liệu tham chiếu hoặc dữ liệu thay đổi theo thời gian; chính Data Platform mới là nơi tích lũy dữ liệu lịch sử và tạo ra quy mô lớn.

## 5. Nhu cầu Phân tích Dữ liệu (Top 10 Business Questions)
Hệ thống Data Platform được thiết kế để trả lời 10 câu hỏi cốt lõi:
1. Doanh thu hôm qua là bao nhiêu?
2. Danh mục sản phẩm nào tăng trưởng nhanh nhất?
3. Tỷ lệ giao hàng đúng hẹn trong 7 ngày qua là bao nhiêu?
4. Đơn vị vận chuyển nào có nhiều đơn trễ nhất?
5. Những ngày mưa có làm tăng thời gian giao hàng không?
6. Khách hàng quay lại chiếm bao nhiêu phần trăm?
7. Sản phẩm nào có đánh giá giảm mạnh trong tuần này?
8. Giá nhập thay đổi ảnh hưởng thế nào đến biên lợi nhuận?
9. Tỉnh/thành nào có tỷ lệ hủy đơn cao nhất?
10. Chiến dịch marketing nào mang lại doanh thu và lợi nhuận tốt nhất?