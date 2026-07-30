# Project Timeline — FastOrder Enterprise E-Commerce Data Platform

**Cập nhật:** 29/07/2026  
**Trạng thái hiện tại:** Initial Olist bootstrap đã hoàn tất.

> Quy ước: Các mốc có ngày chính xác được ghi theo bằng chứng có sẵn. Các mốc không đủ bằng chứng ngày cụ thể được ghi theo khoảng thời gian thay vì tự suy đoán.

---

## Mốc 1 — Khởi động hướng học và nền tảng Airflow

### 23/07/2026

**Mục tiêu**

- Chuẩn bị môi trường orchestration local.
- Hiểu các thành phần của Airflow trước khi đưa vào project.

**Đã thực hiện**

- Thiết lập Airflow 3.3.0 bằng Docker Compose.
- Sử dụng CeleryExecutor, Redis và PostgreSQL metadata database.
- Làm quen với DAG, Scheduler, Worker, API Server, Dag Processor và Triggerer.
- Tắt example DAG.
- Xác nhận các container có thể chạy trong Docker Desktop trên Windows.

**Khó khăn**

- Môi trường Windows/WSL2 có vấn đề đăng ký Ubuntu.
- Phải phân biệt PostgreSQL metadata của Airflow với PostgreSQL nghiệp vụ FastOrder.
- Làm rõ Airflow chỉ orchestration, không nên chứa toàn bộ business logic.

**Kết quả**

- Local Airflow infrastructure sẵn sàng để dùng ở giai đoạn incremental ingestion sau này.

---

## Mốc 2 — Xác định business scope và định hướng platform

### 24–27/07/2026 — khoảng thời gian phục dựng

**Mục tiêu**

- Tránh xây project theo kiểu “CSV → Spark → Dashboard”.
- Thiết kế một data platform có operational source đang thay đổi.

**Đã thực hiện**

- Chọn doanh nghiệp giả lập: **FastOrder**, marketplace B2C tại Việt Nam.
- Quy mô thiết kế: khoảng 50.000 đơn/ngày, nhiều seller và nhiều warehouse.
- Xác định stakeholders: CEO, Sales, Logistics, Inventory, Marketing, Finance và Customer Service.
- Xây business context, stakeholder KPI matrix, happy path, exception paths và order state machine.
- Chốt nguyên tắc: business requirement đi trước tool selection.

**Quyết định chính**

- Olist không phải database cuối cùng của FastOrder.
- Olist chỉ là snapshot seed để khởi tạo operational database.
- Faker simulator sẽ tạo INSERT/UPDATE để database trở thành nguồn OLTP “sống”.

**Kết quả**

- Project có business narrative rõ, phù hợp để giải thích trong CV và phỏng vấn.

---

## Mốc 3 — Hoàn thiện kiến trúc nhiều tầng

### 24–27/07/2026 — khoảng thời gian phục dựng

**Đã thực hiện**

- High-level architecture.
- Logical architecture.
- Physical architecture.
- Data Flow Diagram.
- Chốt target flow:

```text
Source Systems
    → Airflow
    → ADLS Bronze
    → Spark
    → ADLS Silver
    → Synapse
    → dbt Gold
    → Power BI
```

**Lưu ý quan trọng**

- Đây là **target architecture**.
- Current implementation ngày 29/07/2026 mới hoàn tất operational PostgreSQL bootstrap.
- Không được mô tả các thành phần ADLS, Synapse, dbt hay Power BI như đã triển khai.

---

## Mốc 4 — Chốt source systems và data model ban đầu

### 28/07/2026

**Đã thực hiện**

- Operational source:
  - Olist CSV làm seed.
  - PostgreSQL làm FastOrder OLTP.
  - Faker simulator sẽ tạo biến động.
- External APIs:
  - Open-Meteo.
  - Nager.Date.
  - Frankfurter.
  - Mock logistics API.
- File-based sources:
  - Clickstream JSONL.
  - Reviews JSONL theo định hướng ban đầu.
  - Vendor catalog CSV.
- Viết Data Dictionary v2 và business entities.

**Khó khăn phát hiện sau đó**

- Data Dictionary v2 chứa nhiều cột kế hoạch chưa tồn tại trong schema thực tế.
- Một số entity và tên cột không còn đồng bộ với implementation.
- Reviews trong implementation hiện được nạp vào PostgreSQL từ Olist, trong khi tài liệu cũ còn mô tả thêm `reviews.jsonl`.

**Kết quả**

- Có nền tảng tài liệu dữ liệu, nhưng cần nâng lên v3 để khớp schema đã triển khai.

---

## Mốc 5 — Dựng FastOrder PostgreSQL riêng

### Trước và trong ngày 29/07/2026

**Đã thực hiện**

- Tạo PostgreSQL 16 riêng cho FastOrder trong Docker.
- Phân biệt:
  - Airflow metadata PostgreSQL: host port 5432.
  - FastOrder operational PostgreSQL: host port 5433.
- Tải các file Olist vào `data/raw/olist/`.
- Không download dataset khi pipeline runtime.

**Kết quả**

- FastOrder có operational database độc lập với Airflow metadata DB.

---

## Mốc 6 — Thiết kế schema-first

### 29/07/2026

**Mục tiêu**

- Không để Pandas tự tạo schema bằng `if_exists="replace"`.
- Database schema phải do project kiểm soát.

**Đã thực hiện**

- Tạo `database/schema.sql`.
- Đổi bảng từ `olist_*` thành tên nghiệp vụ:
  - `customers`, `orders`, `products`, `sellers`, ...
- Thêm:
  - `warehouses`.
  - `inventory`.
  - `created_at`.
  - `updated_at`.
  - PK, FK và indexes.
- Bỏ:
  - `is_simulated`.
  - `simulation_id`.
- Thiết kế inventory bằng composite PK `(warehouse_id, product_id)`.
- Thêm index trên các khóa ngoại và `updated_at`.

**Khó khăn**

- `warehouses` ban đầu được tạo sau `orders` dù `orders` tham chiếu nó.
- `inventory` ban đầu thiếu `updated_at` nhưng lại tạo index cho cột đó.
- Composite PK geolocation dựa trên ZIP/lat/lng không phù hợp dữ liệu thực tế.

**Điều chỉnh**

- Tạo `warehouses` trước `orders`.
- Thêm timestamps cho `inventory`.
- Chuyển geolocation sang surrogate key để bảo toàn duplicate source records.

---

## Mốc 7 — Tổ chức lại Python package và connection layer

### 29/07/2026

**Đã thực hiện**

- Phân biệt:
  - `database/`: SQL artifacts.
  - `fastorder/db/`: Python code thao tác database.
- Chuyển connection logic:
  - Từ `fastorder/ingestion/db.py`.
  - Sang `fastorder/db/connection.py`.
- Đổi từ raw psycopg2 connection sang SQLAlchemy Core Engine.
- Dùng:
  - `URL.create()`.
  - Environment validation.
  - Engine dùng chung.
  - `SELECT 1` smoke test.

**Bài học**

- SQLAlchemy không thay driver; SQLAlchemy sử dụng psycopg2 bên dưới.
- `create_engine()` tạo Engine theo lazy connection, chưa mở kết nối thật ngay lập tức.
- Airflow DAG nên mỏng; logic tái sử dụng nằm trong `fastorder/`.

---

## Mốc 8 — Tạo script khởi tạo database

### 29/07/2026

**Đã thực hiện**

- Tạo `fastorder/db/init_db.py`.
- Script:
  - Tìm `database/schema.sql`.
  - Đọc UTF-8.
  - Lấy Engine.
  - Thực thi schema trong transaction.
  - Commit khi thành công, rollback khi lỗi.
- Chạy bằng:

```bash
python -m fastorder.db.init_db
```

**Đặc điểm vận hành**

- Đây là development bootstrap/reset script.
- Vì `schema.sql` có `DROP TABLE`, chạy lại sẽ xóa dữ liệu hiện tại.
- Không được đặt script này trong DAG định kỳ.

---

## Mốc 9 — Initial Olist loader

### 29/07/2026

**Đã thực hiện**

- Refactor `load_olist.py` để:
  - Dùng SQLAlchemy Engine.
  - Nạp vào schema đã tạo sẵn.
  - Dùng `if_exists="append"`.
  - Nạp theo thứ tự tôn trọng FK.
  - Kiểm tra đủ file trước khi mở transaction.
  - Dùng một transaction cho toàn bộ bootstrap.
  - Ném lại exception để tiến trình thất bại rõ ràng.
- Load order:

```text
geolocation
product_category_name_translation
customers
products
sellers
orders
order_items
order_payments
order_reviews
```

**Khó khăn**

- Geolocation vi phạm integrity constraint.
- Nguyên nhân: composite PK dựa trên tọa độ không phù hợp duplicate/precision của dữ liệu nguồn.
- Không chọn cách âm thầm `drop_duplicates()` trong loader.
- Sửa schema bằng surrogate key để giữ nguyên dữ liệu raw.

**Kết quả**

- Nạp thành công dữ liệu Olist vào PostgreSQL FastOrder.
- Initial operational snapshot hoàn tất.

---

## Mốc 10 — Post-load validation

### 30/07/2026
**Đã thực hiện**
- Post-load row-count reconciliation hoàn tất.
- Khẳng định foreign-key integrity validation hoàn tất.
- Hoàn thành Required NULL validation (kiểm tra 17 trường bắt buộc không có NULL).
- Quyết định dừng validation ở tầng cấu trúc vật lý, không thực hiện timestamp consistency validation tại file này.

**Kết quả**
- Chốt mốc Post-load validation hoàn tất. Toàn bộ 9/9 bảng khớp dòng, 6/6 quan hệ không orphan, 17/17 cột bắt buộc tuân thủ constraint.
---

## Mốc 11 — Thiết kế mạng lưới và Seed dữ liệu Warehouse

### 30/07/2026
**Đã thực hiện**
- Thiết kế mạng lưới 5 kho hàng chiến lược bao phủ 3 miền tại Việt Nam: Hà Nội, Hải Phòng (NORTH); Đà Nẵng (CENTRAL); TP.HCM, Cần Thơ (SOUTH).
- Xây dựng kịch bản `seed_warehouses.py` ứng dụng cơ chế UPSERT (`ON CONFLICT DO UPDATE`) để đảm bảo tính Idempotent.
- Tích hợp kiểm đếm trạng thái (`SELECT COUNT(*)`) ngay bên trong Transaction (`engine.begin()`) để xác nhận tính toàn vẹn.

**Kết quả**
- Chạy thành công quá trình Warehouse Seed.
- 5 bản ghi kho hàng đã được nạp an toàn vào PostgreSQL. Hạ tầng kho bãi đã hoàn tất và sẵn sàng cho bước phân bổ tồn kho (Inventory).

## Trạng thái chốt ngày 29/07/2026

```text
Business & architecture docs       COMPLETE
Docker/Airflow local infrastructure COMPLETE
FastOrder PostgreSQL               COMPLETE
Schema-first database              COMPLETE
SQLAlchemy connection layer        COMPLETE
Database bootstrap                 COMPLETE
Initial Olist load                 COMPLETE
Post-load validation               COMPLETE
Faker simulator                    NOT STARTED
Airflow incremental ingestion      NOT STARTED
Bronze/Silver/Gold                 NOT STARTED
Power BI                           NOT STARTED
```
