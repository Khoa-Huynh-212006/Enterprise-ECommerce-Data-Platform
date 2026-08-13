# File-Based Ingestion Architecture (YOOCHOOSE Onboarding)

Luồng xử lý file-based data từ các đối tác bên ngoài (ví dụ: YOOCHOOSE clickstream) được thiết kế tuân thủ nguyên tắc Separation of Concerns, chia thành ba giai đoạn với các công cụ chuyên biệt:

## 1. Cloud Movement (Azure Data Factory)
*   **Mục đích:** Đưa artifact từ nguồn bên ngoài (External HTTP/SFTP/S3) vào hệ sinh thái lưu trữ nội bộ (Azure Data Lake).
*   **Triển khai:** Sử dụng ADF Copy Activity để kéo trực tiếp file gốc (ví dụ: `yoochoose-data.7z`) thả vào `landing/bootstrap/`. ADF hoạt động như một cỗ máy vận chuyển thuần túy, không can thiệp vào cấu trúc hay nội dung file.

## 2. Cloud Data Preparation (Azure Databricks)
*   **Mục đích:** Chuẩn bị dữ liệu thô thành định dạng sẵn sàng cho hệ thống Ingestion chính thức.
*   **Triển khai:** 
    *   Cài đặt thư viện Python chuyên dụng (`%pip`) để giải nén archive (`.7z`) trực tiếp trên Cloud Compute.
    *   Sử dụng Apache Spark để phân tích `event_timestamp` và tái cấu trúc (layout) các file thành phân vùng theo thời gian (Time-based layout: `event_date=YYYY-MM-DD/`).
    *   Ghi dữ liệu đã chuẩn bị vào `landing/clickstream/yoochoose/prepared/`.
*   **Bảo mật:** Databricks tương tác với ADLS hoàn toàn thông qua **Access Connector / Managed Identity**, loại bỏ triệt để việc hard-code client secrets hoặc Service Principal keys trong Notebook.

## 3. Data Ingestion (Apache Airflow)
*   **Mục đích:** Quản lý vòng đời đưa file từ Landing vào Bronze Layer với các đặc tính ACID cơ bản (Idempotency, Checkpointing).
*   **Triển khai:** Airflow quét (discovery) thư mục `landing/prepared/`, cập nhật File Manifest, và ingest dữ liệu đã phân mảnh thời gian vào `bronze/`.