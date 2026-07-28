# Physical Architecture (Kiến trúc Vật lý)

## 1. Mục tiêu
Tài liệu này ánh xạ các khối chức năng từ Logical Architecture sang các công nghệ cụ thể được triển khai cho dự án FastOrder Data Platform. Trọng tâm của tài liệu là giải thích **tại sao (Rationale)** lại chọn công nghệ đó để giải quyết bài toán nghiệp vụ, thay vì các giải pháp thay thế khác.

---

## 2. Ánh xạ Công nghệ (Technology Mapping)

| Khối chức năng (Logical) | Công nghệ triển khai (Physical) | Trách nhiệm chính |
| :--- | :--- | :--- |
| **Source Systems** | PostgreSQL, REST APIs, JSON/CSV Files | Hệ thống OLTP cốt lõi và dữ liệu ngoại vi. |
| **Data Ingestion** | **Apache Airflow** (Python Operators) | Lên lịch và điều phối việc kéo dữ liệu từ Source. |
| **Raw & Curated Zone** | **Azure Data Lake Storage Gen2** (ADLS Gen2) | Lưu trữ phân lớp theo Medallion (Bronze, Silver). |
| **Data Processing** | **Apache Spark** (PySpark) | Xử lý Big Data, làm sạch và chuẩn hóa (Bronze ➔ Silver). |
| **Analytics Storage** | **Azure Synapse Analytics** | Lưu trữ Data Warehouse (Gold Layer / Data Marts). |
| **Modeling & Transform** | **dbt** (Data Build Tool) | Biến đổi dữ liệu có cấu trúc bằng SQL bên trong Synapse (Silver ➔ Gold). |
| **Workflow Orchestration**| **Apache Airflow** (Dockerized) | Quản lý vòng đời toàn bộ DAGs (Ingest ➔ Spark ➔ dbt). |

---

## 3. Lập luận Quyết định Kiến trúc (Architecture Decisions)

### 3.1. Tại sao Ingestion dùng Airflow mà không dùng Azure Data Factory (ADF)?
* **Tính linh hoạt (Code-as-Configuration):** Airflow cho phép định nghĩa pipeline hoàn toàn bằng Python code, rất phù hợp để xử lý các logic ingestion phức tạp từ API (như xử lý rate limit, pagination) mà giao diện kéo thả của ADF khó tối ưu bằng.
* **Môi trường giả lập (Local Development):** Dự án cần triển khai mượt mà trên Docker ở local. Airflow hoàn toàn mã nguồn mở và chạy tốt trên Docker, trong khi ADF là dịch vụ Cloud-native (bị khóa vào hệ sinh thái Azure) và khó test offline, đồng thời phát sinh chi phí ngay từ khâu phát triển.

### 3.2. Tại sao Raw Zone dùng Azure Data Lake Storage Gen2?
* **Hierarchical Namespace:** ADLS Gen2 hỗ trợ cấu trúc thư mục thực sự (thay vì thư mục ảo như S3 hay Blob Storage thông thường). Điều này mang tính sống còn để tổ chức Data Lake theo kiến trúc Medallion (ví dụ: `bronze/orders/yyyy/mm/dd`).
* **Hiệu suất & Tích hợp:** Chuẩn giao tiếp ABFS (Azure Blob File System) của ADLS Gen2 được tối ưu hóa đặc biệt cho Apache Spark, giúp các luồng xử lý big data đọc/ghi với tốc độ rất cao.

### 3.3. Tại sao Processing lại dùng Apache Spark?
* **Khả năng mở rộng (Scalability):** Dữ liệu từ hệ thống E-commerce (50.000 đơn/ngày, cộng thêm Clickstream và Reviews) sẽ phình to rất nhanh. Spark xử lý phân tán trong bộ nhớ (In-memory), dễ dàng scan và parse hàng triệu dòng JSON/Parquet hiệu quả hơn nhiều so với việc dùng Pandas hay Python thuần.
* **Định hình dữ liệu thô:** Spark rất mạnh trong việc áp dụng schema (Schema-on-read), loại bỏ dữ liệu hỏng (corrupted records), và lưu trữ lại dưới định dạng columnar như Parquet để tối ưu hóa cho các bước sau.

### 3.4. Tại sao Warehouse lại là Azure Synapse Analytics?
* **Chuyên trị OLAP:** Synapse sở hữu MPP (Massively Parallel Processing) engine, được sinh ra để quét các Fact tables hàng trăm triệu dòng và join với các Dimension tables trong mô hình Star Schema chỉ trong vài giây.
* **Hệ sinh thái đồng nhất:** Nó kết nối xuyên suốt với ADLS Gen2 (để query trực tiếp dữ liệu Silver qua Serverless SQL) và tích hợp hoàn hảo với Power BI cho tầng Data Consumption.

### 3.5. Tại sao dbt lại nằm sau Spark chứ không phải trước?
Đây là sự phân tách trách nhiệm giữa **Heavy Compute** và **Business Logic**:
* **Spark đi trước (Bronze ➔ Silver):** Đảm nhiệm các công việc "nặng nhọc" ở tầng File system: giải nén, parse JSON phức tạp, xử lý deduplication, và ép kiểu dữ liệu. Kết quả sinh ra bảng Silver sạch sẽ (dạng Parquet).
* **dbt đi sau (Silver ➔ Gold):** Khi dữ liệu đã sạch và được nạp (hoặc expose) vào Synapse, dbt sẽ tiếp quản. Vì dbt chỉ dùng SQL, nó cực kỳ phù hợp để xây dựng Data Models (Fact/Dim), áp dụng các logic tính toán (như commission, profit margin), và thực hiện Data Quality Tests. Nếu dbt đi trước, nó sẽ không thể xử lý tốt các file JSON/CSV phi cấu trúc nằm trong Data Lake.

---

## 4. Luồng di chuyển Dữ liệu (Data Movement)

Dữ liệu di chuyển qua các thành phần theo thứ tự sau (được Airflow gọi tuần tự):

1. **Ingest (Extract & Load):** Airflow chạy Python Operators gọi API và kết nối DB ➔ Kéo dữ liệu thô đẩy thẳng vào `ADLS Gen2 (Bronze)`.
2. **Process (Transform 1):** Airflow kích hoạt Spark Job (kết nối với ADLS Gen2). Spark đọc dữ liệu từ thư mục `Bronze`, làm sạch, đổi định dạng sang Parquet và ghi xuống thư mục `Silver`.
3. **Load to DWH:** Synapse dùng PolyBase (hoặc Copy Activity) đọc dữ liệu Parquet từ thư mục `Silver` và nạp vào các Staging Tables bên trong Data Warehouse.
4. **Modeling (Transform 2):** Airflow gọi `dbt run`. dbt thực thi các câu lệnh SQL bên trong Synapse để biến đổi Staging Tables thành các bảng Fact và Dimension (Gold Layer / Data Marts).