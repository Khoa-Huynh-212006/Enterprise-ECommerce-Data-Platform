# Enterprise E-Commerce Data Platform
> Dự án Data Engineering mô phỏng một nền tảng dữ liệu hiện đại cho doanh nghiệp thương mại điện tử, được thiết kế theo tiêu chuẩn triển khai thực tế (Production-ready).

# 1. Giới thiệu nổi bật

---

# 2. Tổng quan dự án
Dự án này xây dựng một Data Platform toàn diện cho FastOrder – một nền tảng thương mại điện tử mô hình Marketplace B2C. Hệ thống đảm nhiệm việc thu thập, lưu trữ, xử lý và chuyển đổi khối lượng dữ liệu lớn (khoảng 50.000 đơn hàng/ngày) từ nhiều nguồn khác nhau thành các Data Mart sẵn sàng cho phân tích.

Bằng cách áp dụng các luồng dữ liệu tự động (Data Pipelines) và chuẩn hóa theo kiến trúc Medallion (Bronze - Silver - Gold), nền tảng cung cấp cho ban lãnh đạo và các phòng ban (Sales, Logistics, Inventory, Marketing...) một nguồn sự thật duy nhất (Single Source of Truth) để đưa ra các quyết định kinh doanh dựa trên dữ liệu.
---

# 3. Bối cảnh nghiệp vụ
Xác định rõ "bài toán" cần giải quyết trước khi thiết kế hệ thống.
* 📄 [Tổng quan Doanh nghiệp & Hệ thống nguồn](01-business/business-context.md)
* 📄 [Ma trận Stakeholders & Yêu cầu KPIs](01-business/stakeholders-kpis.md)
---

# 4. Mục tiêu dự án
- Xây dựng nền tảng dữ liệu tập trung: Khởi tạo Data Lake/Data Warehouse trên Azure để phá vỡ các silo dữ liệu từ Database, API và File systems.

- Mô phỏng kiến trúc Enterprise Data Engineering: Triển khai quy trình ETL/ELT theo tiêu chuẩn thực tế bằng cách sử dụng Airflow (chạy trên Docker) để điều phối các DAGs, Spark để xử lý dữ liệu lớn và dbt để transform dữ liệu.

- Đảm bảo chất lượng và chuẩn hóa dữ liệu: Thiết kế các Data Model tối ưu cho phân tích (Star Schema/Snowflake) và quản lý chặt chẽ theo các phân lớp Bronze (Raw), Silver (Cleansed), Gold (Aggregated).

- Thúc đẩy Data-Driven Decision Making: Cung cấp dữ liệu đã được làm sạch và mô hình hóa cho các công cụ BI (Power BI), phục vụ trực tiếp bộ chỉ số KPI cho Executive, Sales, Logistics, Inventory, Marketing, Finance và Customer Service.
---

# 5. Phạm vi dự án


### Bao gồm (In Scope)


### Không bao gồm (Out of Scope)


---

# 6. Kiến trúc tổng quan


---

# 7. Các chức năng chính

---

# 8. Công nghệ sử dụng và lý do lựa chọn

---

# 9. Cấu trúc Repository

```text
Enterprise-ECommerce-Data-Platform/
├── config/                  # Cấu hình hệ thống, variables và credentials
├── data/                    # Mock data và scripts sinh dữ liệu giả lập
├── docker/                  # Dockerfile và docker-compose.yml
├── docs/                    # Tài liệu dự án chi tiết
│   ├── 01-business/         # Bối cảnh, quy trình nghiệp vụ và KPIs
│   ├── 02-architecture/     # Thiết kế kiến trúc tổng thể
│   ├── 03-data/             # Data models, Data dictionary
│   ├── 04-pipelines/        # Thiết kế luồng xử lý ETL/ELT
│   ├── 05-operations/       # Hướng dẫn vận hành, monitoring
│   ├── 06-decisions/        # Architecture Decision Records (ADRs)
│   ├── 07-progress/         # Roadmap và tiến độ dự án
│   └── development/         # Hướng dẫn setup cho Developer
├── scripts/                 # Bash/Python scripts hỗ trợ CI/CD và setup
├── src/                     # Mã nguồn chính (Airflow DAGs, Spark jobs, dbt models)
├── tests/                   # Unit tests và Data tests (Data Quality)
└── README.md                # Tài liệu tổng quan của dự án
```

---

# 10. Lộ trình phát triển

---

# 11. Tài liệu

---

# 12. Tiến độ hiện tại

---

# 13. Hướng dẫn bắt đầu nhanh

---

# 14. Giấy phép

