# Từ điển Dữ liệu (Data Dictionary v2)

Tài liệu này định nghĩa cấu trúc chi tiết của các thực thể (Entities) trong hệ thống FastOrder. Mô hình dữ liệu kết hợp giữa bộ dữ liệu thực tế Olist và các trường dữ liệu giả lập (Simulator) để mô phỏng một hệ thống giao dịch (OLTP) đang hoạt động.

**Quy ước chung (Naming Conventions):**
*   Tất cả tên bảng và cột sử dụng định dạng `snake_case`.
*   Cột lưu trữ thời gian cụ thể dùng kiểu `TIMESTAMP` với hậu tố `_at`.
*   Cột lưu trữ ngày tháng dùng kiểu `DATE` với hậu tố `_date`.
*   Tiền tệ và các số liệu đo lường chính xác dùng `NUMERIC(precision, scale)`.

---

## 1. Nhóm Giao dịch (Core Transactions - PostgreSQL)

### Bảng: `customers`
*   **Mô tả:** Lưu trữ thông tin định danh và liên lạc của khách hàng.

| Tên cột | Kiểu dữ liệu | Ràng buộc | Nguồn gốc | Ghi chú / Ví dụ |
| :--- | :--- | :--- | :--- | :--- |
| `customer_id` | VARCHAR(50) | PK, NOT NULL | Olist | Mã định danh khách hàng trên hệ thống |
| `customer_unique_id` | VARCHAR(50) | UNIQUE, NOT NULL| Olist | Mã định danh duy nhất (CCCD/Tax ID) |
| `customer_name` | VARCHAR(100)| NOT NULL | Simulator | Tên khách hàng |
| `gender` | VARCHAR(10) | | Simulator | `M`, `F`, `Other` |
| `birth_date` | DATE | | Simulator | Ngày sinh |
| `email` | VARCHAR(150)| UNIQUE | Simulator | Email liên hệ |
| `phone` | VARCHAR(20) | | Simulator | Số điện thoại |
| `city` | VARCHAR(50) | NOT NULL | Olist | Thành phố cư trú |
| `province` | VARCHAR(50) | NOT NULL | Olist | Tỉnh/Bang |
| `created_source` | VARCHAR(20) | NOT NULL | Simulator | `APP`, `WEB`, `MARKETING`, `SOCIAL` |
| `created_at` | TIMESTAMP | NOT NULL | Derived | Thời điểm tạo tài khoản |
| `updated_at` | TIMESTAMP | NOT NULL | Derived | Cập nhật khi thay đổi profile (CDC) |

### Bảng: `orders`
*   **Mô tả:** Ghi nhận thông tin tổng thể của một đơn hàng.

| Tên cột | Kiểu dữ liệu | Ràng buộc | Nguồn gốc | Ghi chú / Ví dụ |
| :--- | :--- | :--- | :--- | :--- |
| `order_id` | VARCHAR(50) | PK, NOT NULL | Olist | Mã đơn hàng |
| `customer_id` | VARCHAR(50) | FK, NOT NULL | Olist | Khóa ngoại trỏ đến `customers` |
| `warehouse_id` | VARCHAR(50) | FK, NOT NULL | Custom | Khóa ngoại trỏ đến `warehouses` |
| `order_status` | VARCHAR(30) | NOT NULL | Simulator | `pending_payment`, `processing`, `shipped`... |
| `total_amount` | NUMERIC(10,2)| NOT NULL | Derived | Tổng giá trị hóa đơn |
| `shipping_provider`| VARCHAR(50) | | Simulator | Tên đơn vị vận chuyển |
| `order_created_at` | TIMESTAMP | NOT NULL | Olist | Thời điểm chốt đơn |
| `updated_at` | TIMESTAMP | NOT NULL | Simulator | Cập nhật theo sự kiện đổi trạng thái |

### Bảng: `order_items`
*   **Mô tả:** Chi tiết từng sản phẩm được mua bên trong một đơn hàng.

| Tên cột | Kiểu dữ liệu | Ràng buộc | Nguồn gốc | Ghi chú / Ví dụ |
| :--- | :--- | :--- | :--- | :--- |
| `order_item_id` | VARCHAR(50) | PK, NOT NULL | Olist | Mã chi tiết đơn hàng |
| `order_id` | VARCHAR(50) | FK, NOT NULL | Olist | Khóa ngoại trỏ đến `orders` |
| `product_id` | VARCHAR(50) | FK, NOT NULL | Olist | Khóa ngoại trỏ đến `products` |
| `seller_id` | VARCHAR(50) | FK, NOT NULL | Olist | Khóa ngoại trỏ đến `sellers` |
| `quantity` | INTEGER | NOT NULL | Simulator | Số lượng mua |
| `price` | NUMERIC(10,2)| NOT NULL | Olist | Đơn giá sản phẩm |
| `freight_value` | NUMERIC(10,2)| NOT NULL | Olist | Phí vận chuyển phân bổ cho món hàng |
| `created_at` | TIMESTAMP | NOT NULL | Derived | Thời điểm tạo chi tiết đơn |
| `updated_at` | TIMESTAMP | NOT NULL | Derived | Phục vụ quét CDC |

### Bảng: `products`
*   **Mô tả:** Thông tin chi tiết về sản phẩm.

| Tên cột | Kiểu dữ liệu | Ràng buộc | Nguồn gốc | Ghi chú / Ví dụ |
| :--- | :--- | :--- | :--- | :--- |
| `product_id` | VARCHAR(50) | PK, NOT NULL | Olist | Mã sản phẩm |
| `category_id` | VARCHAR(50) | FK, NOT NULL | Custom | Khóa ngoại trỏ đến `product_categories` |
| `product_name` | VARCHAR(255)| NOT NULL | Olist | Tên sản phẩm |
| `brand` | VARCHAR(100)| | Simulator | Thương hiệu |
| `weight_g` | INTEGER | | Olist | Trọng lượng (gram) |
| `length_cm` | INTEGER | | Olist | Chiều dài (cm) |
| `height_cm` | INTEGER | | Olist | Chiều cao (cm) |
| `width_cm` | INTEGER | | Olist | Chiều rộng (cm) |
| `created_at` | TIMESTAMP | NOT NULL | Derived | Ngày đăng bán |
| `updated_at` | TIMESTAMP | NOT NULL | Derived | Ngày cập nhật thông tin gần nhất |

### Bảng: `product_categories`
*   **Mô tả:** Danh mục ngành hàng.

| Tên cột | Kiểu dữ liệu | Ràng buộc | Nguồn gốc | Ghi chú / Ví dụ |
| :--- | :--- | :--- | :--- | :--- |
| `category_id` | VARCHAR(50) | PK, NOT NULL | Custom | Mã danh mục |
| `category_name_vn` | VARCHAR(100)| NOT NULL | Olist | Tên danh mục (Tiếng Việt) |
| `category_name_en` | VARCHAR(100)| NOT NULL | Olist | Tên danh mục (Tiếng Anh) |

### Bảng: `sellers`
*   **Mô tả:** Thông tin đối tác cung cấp (Nhà bán hàng).

| Tên cột | Kiểu dữ liệu | Ràng buộc | Nguồn gốc | Ghi chú / Ví dụ |
| :--- | :--- | :--- | :--- | :--- |
| `seller_id` | VARCHAR(50) | PK, NOT NULL | Olist | Mã nhà bán hàng |
| `seller_name` | VARCHAR(150)| NOT NULL | Simulator | Tên gian hàng |
| `city` | VARCHAR(50) | NOT NULL | Olist | Thành phố |
| `province` | VARCHAR(50) | NOT NULL | Olist | Tỉnh / Bang |
| `rating` | NUMERIC(3,2) | | Simulator | Điểm đánh giá trung bình |
| `created_at` | TIMESTAMP | NOT NULL | Derived | Ngày đăng ký bán hàng |
| `updated_at` | TIMESTAMP | NOT NULL | Derived | Cập nhật hồ sơ nhà bán |

### Bảng: `payments`
*   **Mô tả:** Lịch sử các giao dịch thanh toán cho đơn hàng.

| Tên cột | Kiểu dữ liệu | Ràng buộc | Nguồn gốc | Ghi chú / Ví dụ |
| :--- | :--- | :--- | :--- | :--- |
| `payment_id` | VARCHAR(50) | PK, NOT NULL | Custom | Mã giao dịch thanh toán |
| `order_id` | VARCHAR(50) | FK, NOT NULL | Olist | Khóa ngoại trỏ đến `orders` |
| `payment_sequential`| INTEGER | NOT NULL | Olist | Thứ tự thanh toán (nếu tách bill) |
| `payment_type` | VARCHAR(30) | NOT NULL | Olist | `credit_card`, `momo`, `voucher`... |
| `payment_installments`| INTEGER | NOT NULL | Olist | Số tháng trả góp |
| `payment_value` | NUMERIC(10,2)| NOT NULL | Olist | Số tiền thanh toán |
| `payment_status` | VARCHAR(30) | NOT NULL | Simulator | `success`, `failed`, `pending` |
| `created_at` | TIMESTAMP | NOT NULL | Derived | Thời điểm tạo phiên thanh toán |
| `updated_at` | TIMESTAMP | NOT NULL | Simulator | Cập nhật khi có kết quả từ Gateway |

### Bảng: `warehouses`
*   **Mô tả:** Quản lý danh sách các kho hàng trung chuyển.

| Tên cột | Kiểu dữ liệu | Ràng buộc | Nguồn gốc | Ghi chú / Ví dụ |
| :--- | :--- | :--- | :--- | :--- |
| `warehouse_id` | VARCHAR(50) | PK, NOT NULL | Custom | Mã kho hàng |
| `warehouse_name` | VARCHAR(100)| NOT NULL | Custom | Tên kho |
| `city` | VARCHAR(50) | NOT NULL | Custom | Thành phố đặt kho |
| `province` | VARCHAR(50) | NOT NULL | Custom | Tỉnh / Bang |
| `capacity` | INTEGER | NOT NULL | Custom | Sức chứa tối đa |
| `manager_name` | VARCHAR(100)| | Custom | Tên quản lý kho |

### Bảng: `inventory`
*   **Mô tả:** Lưu trữ trạng thái xuất/nhập/tồn kho hiện tại.

| Tên cột | Kiểu dữ liệu | Ràng buộc | Nguồn gốc | Ghi chú / Ví dụ |
| :--- | :--- | :--- | :--- | :--- |
| `inventory_id` | VARCHAR(50) | PK, NOT NULL | Custom | Mã kiểm kho |
| `warehouse_id` | VARCHAR(50) | FK, NOT NULL | Custom | Khóa ngoại trỏ đến `warehouses` |
| `product_id` | VARCHAR(50) | FK, NOT NULL | Custom | Khóa ngoại trỏ đến `products` |
| `inventory_transactions`| INTEGER| NOT NULL | Custom | Lượng thay đổi xuất/nhập (Delta) |
| `last_restock_date`| DATE | | Custom | Ngày nhập hàng gần nhất |
| `updated_at` | TIMESTAMP | NOT NULL | Custom | Cập nhật khi có biến động kho |

---

## 2. Nhóm Hành vi & Đối tác (File-based Systems)

### File: `clickstream.jsonl`
*   **Mô tả:** Dữ liệu hành vi người dùng trên hệ thống.

| Tên cột | Kiểu dữ liệu | Ràng buộc | Nguồn gốc | Ghi chú / Ví dụ |
| :--- | :--- | :--- | :--- | :--- |
| `event_id` | VARCHAR(50) | PK, NOT NULL | Simulator | Mã sự kiện sinh ra ngẫu nhiên |
| `session_id` | VARCHAR(50) | NOT NULL | Simulator | Mã phiên truy cập Web/App |
| `customer_id` | VARCHAR(50) | FK | Simulator | Khách chưa đăng nhập sẽ có giá trị NULL |
| `product_id` | VARCHAR(50) | FK | Simulator | Mã sản phẩm được tương tác |
| `event_type` | VARCHAR(30) | NOT NULL | Simulator | `view`, `add_to_cart`, `checkout` |
| `device_type` | VARCHAR(30) | NOT NULL | Simulator | `mobile`, `desktop`, `tablet` |
| `event_timestamp` | TIMESTAMP | NOT NULL | Simulator | Thời điểm ghi nhận log |

### File: `reviews.jsonl`
*   **Mô tả:** Đánh giá từ khách hàng.

| Tên cột | Kiểu dữ liệu | Ràng buộc | Nguồn gốc | Ghi chú / Ví dụ |
| :--- | :--- | :--- | :--- | :--- |
| `review_id` | VARCHAR(50) | PK, NOT NULL | Olist | Mã lượt đánh giá |
| `order_id` | VARCHAR(50) | FK, NOT NULL | Olist | Thuộc về đơn hàng nào |
| `product_id` | VARCHAR(50) | FK, NOT NULL | Custom | Sản phẩm bị đánh giá |
| `customer_id` | VARCHAR(50) | FK, NOT NULL | Custom | Khách hàng viết review |
| `review_score` | INTEGER | NOT NULL | Olist | Số sao (1-5) |
| `review_title` | VARCHAR(255)| | Olist | Tiêu đề đánh giá |
| `review_comment` | TEXT | | Olist | Nội dung bình luận chi tiết |
| `review_creation_date`| TIMESTAMP| NOT NULL | Olist | Thời điểm đăng đánh giá |

### File: `vendor_catalog.csv`
*   **Mô tả:** File báo giá định kỳ từ đối tác cung cấp.

| Tên cột | Kiểu dữ liệu | Ràng buộc | Nguồn gốc | Ghi chú / Ví dụ |
| :--- | :--- | :--- | :--- | :--- |
| `catalog_id` | VARCHAR(50) | PK, NOT NULL | Custom | Mã bản ghi báo giá |
| `seller_id` | VARCHAR(50) | FK, NOT NULL | Olist | Nhà cung cấp |
| `product_id` | VARCHAR(50) | FK, NOT NULL | Olist | Mã sản phẩm |
| `cost_price` | NUMERIC(10,2)| NOT NULL | Simulator | Giá vốn / Giá nhập vào |
| `effective_date` | DATE | NOT NULL | Simulator | Ngày bắt đầu áp dụng |
| `end_date` | DATE | | Simulator | Ngày kết thúc hiệu lực |

---

## 3. Nhóm Ngữ cảnh (External APIs)

### Bảng Raw: `weather_api_data`
*   **Nguồn:** Open-Meteo

| Tên cột | Kiểu dữ liệu | Ràng buộc | Nguồn gốc | Ghi chú / Ví dụ |
| :--- | :--- | :--- | :--- | :--- |
| `weather_id` | VARCHAR(50) | PK, NOT NULL | External API| ID dòng thời tiết |
| `city` | VARCHAR(50) | NOT NULL | Custom | Trùng khớp với thành phố có kho hàng |
| `date` | DATE | NOT NULL | External API| Ngày ghi nhận |
| `temperature_avg` | NUMERIC(5,2) | | External API| Nhiệt độ trung bình (độ C) |
| `precipitation_mm` | NUMERIC(5,2) | | External API| Lượng mưa (mm) |
| `weather_condition` | VARCHAR(50) | | External API| `Rain`, `Sunny`, `Cloudy` |

### Bảng Raw: `exchange_rates_data`
*   **Nguồn:** Frankfurter

| Tên cột | Kiểu dữ liệu | Ràng buộc | Nguồn gốc | Ghi chú / Ví dụ |
| :--- | :--- | :--- | :--- | :--- |
| `rate_id` | VARCHAR(50) | PK, NOT NULL | External API| ID dòng tỷ giá |
| `base_currency` | VARCHAR(10) | NOT NULL | External API| Ví dụ: `USD`, `CNY` |
| `target_currency` | VARCHAR(10) | NOT NULL | External API| `VND` |
| `exchange_rate` | NUMERIC(10,4)| NOT NULL | External API| Giá trị quy đổi |
| `date` | DATE | NOT NULL | External API| Ngày tra cứu tỷ giá |

### Bảng Raw: `shipping_status_data`
*   **Nguồn:** Mock Logistics API

| Tên cột | Kiểu dữ liệu | Ràng buộc | Nguồn gốc | Ghi chú / Ví dụ |
| :--- | :--- | :--- | :--- | :--- |
| `tracking_id` | VARCHAR(50) | PK, NOT NULL | Mock API | Mã vận đơn |
| `order_id` | VARCHAR(50) | FK, NOT NULL | Olist | Tham chiếu về bảng Orders |
| `carrier_name` | VARCHAR(100)| NOT NULL | Mock API | Ví dụ: `FastExpress`, `EcoShip` |
| `shipping_status` | VARCHAR(50) | NOT NULL | Mock API | `in_transit`, `delayed`, `delivered` |
| `estimated_delivery_date`| TIMESTAMP| | Mock API | Thời gian giao dự kiến |
| `actual_delivery_date`| TIMESTAMP | | Mock API | Thời gian giao thực tế |
| `delay_reason` | VARCHAR(255)| | Mock API | Lý do (nếu bị delay) |
| `updated_at` | TIMESTAMP | NOT NULL | Mock API | Lần cập nhật lộ trình cuối cùng |

### Bảng Raw: `holidays_data`
*   **Nguồn:** Nager.Date

| Tên cột | Kiểu dữ liệu | Ràng buộc | Nguồn gốc | Ghi chú / Ví dụ |
| :--- | :--- | :--- | :--- | :--- |
| `holiday_id` | VARCHAR(50) | PK, NOT NULL | External API| ID ngày lễ |
| `date` | DATE | NOT NULL | External API| Ngày lễ cụ thể |
| `holiday_name` | VARCHAR(255)| NOT NULL | External API| Ví dụ: `Vietnamese New Year` |
| `is_national` | BOOLEAN | NOT NULL | External API| `True` (Quốc lễ) hoặc `False` |