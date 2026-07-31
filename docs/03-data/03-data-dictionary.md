# Data Dictionary v3 — Implemented PostgreSQL Schema

**Effective date:** 29/07/2026  
**Source of truth:** `database/schema.sql`

> Bản v3 chỉ mô tả schema PostgreSQL đã triển khai và nạp dữ liệu thành công. Các cột simulator dự kiến nhưng chưa tồn tại không được ghi như đã implemented.

## Conventions

- Table/column: `snake_case`.
- Time: `TIMESTAMP`.
- Money/measurement: `DECIMAL`.
- `created_at`, `updated_at`: default `CURRENT_TIMESTAMP` khi INSERT.
- UPDATE không tự thay đổi `updated_at`; writer phải cập nhật rõ ràng hoặc dùng trigger.

---

## `geolocation`

| Column | Type | Constraint | Source | Note |
|---|---|---|---|---|
| `geolocation_id` | BIGINT | PK, identity | FastOrder | Surrogate key |
| `geolocation_zip_code_prefix` | VARCHAR(10) |  | Olist | ZIP prefix |
| `geolocation_lat` | DECIMAL(10,6) |  | Olist | Latitude |
| `geolocation_lng` | DECIMAL(10,6) |  | Olist | Longitude |
| `geolocation_city` | VARCHAR(100) |  | Olist | City |
| `geolocation_state` | VARCHAR(2) |  | Olist | State code |
| `created_at` | TIMESTAMP | default current timestamp | Derived | Load time |
| `updated_at` | TIMESTAMP | default current timestamp | Derived | Change watermark |

**Rationale:** Giữ duplicate source rows; không dùng tọa độ làm natural key.

---

## `customers`

| Column | Type | Constraint | Source |
|---|---|---|---|
| `customer_id` | VARCHAR(50) | PK | Olist |
| `customer_unique_id` | VARCHAR(50) | NOT NULL | Olist |
| `customer_zip_code_prefix` | VARCHAR(10) |  | Olist |
| `customer_city` | VARCHAR(100) |  | Olist |
| `customer_state` | VARCHAR(2) |  | Olist |
| `created_at` | TIMESTAMP | default current timestamp | Derived |
| `updated_at` | TIMESTAMP | default current timestamp, indexed | Derived |

---

## `warehouses`

| Column | Type | Constraint | Source |
|---|---|---|---|
| `warehouse_id` | VARCHAR(50) | PK | Custom |
| `warehouse_city` | VARCHAR(100) |  | Custom |
| `warehouse_region` | VARCHAR(20) |  | Custom |
| `created_at` | TIMESTAMP | default current timestamp | Derived |
| `updated_at` | TIMESTAMP | default current timestamp, indexed | Derived |

**Current status:** Table exists but is not populated by Olist loader.

---

## `orders`

| Column | Type | Constraint | Source |
|---|---|---|---|
| `order_id` | VARCHAR(50) | PK | Olist |
| `customer_id` | VARCHAR(50) | NOT NULL, FK → customers | Olist |
| `order_status` | VARCHAR(20) | NOT NULL | Olist |
| `order_purchase_timestamp` | TIMESTAMP | NOT NULL | Olist |
| `order_approved_at` | TIMESTAMP |  | Olist |
| `order_delivered_carrier_date` | TIMESTAMP |  | Olist |
| `order_delivered_customer_date` | TIMESTAMP |  | Olist |
| `order_estimated_delivery_date` | TIMESTAMP |  | Olist |
| `created_at` | TIMESTAMP | default current timestamp | Derived |
| `updated_at` | TIMESTAMP | default current timestamp, indexed | Derived |
| `warehouse_id` | VARCHAR(50) | nullable, FK → warehouses | Custom |

---

## `products`

| Column | Type | Constraint | Source |
|---|---|---|---|
| `product_id` | VARCHAR(50) | PK | Olist |
| `product_category_name` | VARCHAR(100) | indexed | Olist |
| `product_name_lenght` | INT |  | Olist |
| `product_description_lenght` | INT |  | Olist |
| `product_photos_qty` | INT |  | Olist |
| `product_weight_g` | DECIMAL(10,2) |  | Olist |
| `product_length_cm` | DECIMAL(10,2) |  | Olist |
| `product_height_cm` | DECIMAL(10,2) |  | Olist |
| `product_width_cm` | DECIMAL(10,2) |  | Olist |
| `created_at` | TIMESTAMP | default current timestamp | Derived |
| `updated_at` | TIMESTAMP | default current timestamp, indexed | Derived |

> `lenght` được giữ theo tên cột Olist để initial loader không phải rename trong bootstrap.

---

## `sellers`

| Column | Type | Constraint | Source |
|---|---|---|---|
| `seller_id` | VARCHAR(50) | PK | Olist |
| `seller_zip_code_prefix` | VARCHAR(10) |  | Olist |
| `seller_city` | VARCHAR(100) |  | Olist |
| `seller_state` | VARCHAR(2) |  | Olist |
| `created_at` | TIMESTAMP | default current timestamp | Derived |
| `updated_at` | TIMESTAMP | default current timestamp, indexed | Derived |

---

## Table: `order_items`
**Description:** Chi tiết các sản phẩm trong một đơn hàng.

| Column Name | Data Type | PK/FK | Nullable | Description |
| :--- | :--- | :--- | :--- | :--- |
| `order_id` | VARCHAR(50) | PK, FK | NO | Mã đơn hàng (Tham chiếu `orders`). |
| `order_item_id` | INT | PK | NO | Số thứ tự (line number) của sản phẩm trong đơn hàng. |
| `product_id` | VARCHAR(50) | FK | NO | Mã sản phẩm (Tham chiếu `products`). |
| `seller_id` | VARCHAR(50) | FK | NO | Mã nhà bán hàng (Tham chiếu `sellers`). |
| `shipping_limit_date`| TIMESTAMP | - | NO | Hạn chót giao hàng cho đơn vị vận chuyển. |
| `price` | DECIMAL(10,2) | - | NO | Đơn giá của 1 sản phẩm. |
| `freight_value` | DECIMAL(10,2) | - | NO | Phí vận chuyển cho line item này. |
| `quantity` | INT | - | NO | Số lượng mua của sản phẩm này (DEFAULT 1). |
| `created_at` | TIMESTAMP | - | NO | Thời điểm tạo bản ghi. |
| `updated_at` | TIMESTAMP | - | NO | Thời điểm cập nhật cuối cùng. |
---

## `order_reviews`

| Column | Type | Constraint | Source |
|---|---|---|---|
| `review_id` | VARCHAR(50) | PK part | Olist |
| `order_id` | VARCHAR(50) | PK part, FK → orders | Olist |
| `review_score` | INT |  | Olist |
| `review_comment_title` | VARCHAR(255) |  | Olist |
| `review_comment_message` | TEXT |  | Olist |
| `review_creation_date` | TIMESTAMP |  | Olist |
| `review_answer_timestamp` | TIMESTAMP |  | Olist |
| `created_at` | TIMESTAMP | default current timestamp | Derived |
| `updated_at` | TIMESTAMP | default current timestamp, indexed | Derived |

---

## `order_payments`

| Column | Type | Constraint | Source |
|---|---|---|---|
| `order_id` | VARCHAR(50) | PK part, FK → orders | Olist |
| `payment_sequential` | INT | PK part | Olist |
| `payment_type` | VARCHAR(20) | NOT NULL | Olist |
| `payment_installments` | INT | NOT NULL | Olist |
| `payment_value` | DECIMAL(10,2) | NOT NULL | Olist |
| `created_at` | TIMESTAMP | default current timestamp | Derived |
| `updated_at` | TIMESTAMP | default current timestamp, indexed | Derived |

---

## `product_category_name_translation`

| Column | Type | Constraint | Source |
|---|---|---|---|
| `product_category_name` | VARCHAR(100) | PK | Olist |
| `product_category_name_english` | VARCHAR(100) | NOT NULL | Olist |
| `created_at` | TIMESTAMP | default current timestamp | Derived |
| `updated_at` | TIMESTAMP | default current timestamp, indexed | Derived |

---

## `inventory`

| Column | Type | Constraint | Source |
|---|---|---|---|
| `warehouse_id` | VARCHAR(50) | PK part, FK → warehouses | Custom |
| `product_id` | VARCHAR(50) | PK part, FK → products | Custom |
| `quantity_available` | INT | NOT NULL, default 0 | Custom |
| `created_at` | TIMESTAMP | default current timestamp | Derived |
| `updated_at` | TIMESTAMP | default current timestamp, indexed | Derived |

**Current status:** Table exists but is not populated by Olist loader.

---

## Index summary

- FK lookup indexes:
  - `orders.customer_id`
  - `order_items.order_id`
  - `order_items.product_id`
  - `order_items.seller_id`
  - `order_items.warehouse_id`
  - `order_reviews.order_id`
  - `order_payments.order_id`
  - inventory warehouse/product
- Incremental lookup indexes:
  - `updated_at` on operational tables.
- Product category index:
  - `products.product_category_name`.
