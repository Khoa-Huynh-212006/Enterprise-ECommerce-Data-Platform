# Kiến trúc Tổng quan (High-Level Architecture)

## 1. Mục tiêu

Tài liệu này mô tả **bức tranh tổng thể** của FastOrder Data Platform ở mức năng lực (capability), không phụ thuộc vào một công nghệ cụ thể. Các lựa chọn công nghệ hiện tại được mô tả tại `03-physical-architecture.md`.

FastOrder cần trả lời ba câu hỏi:

1. Dữ liệu đến từ đâu?
2. Dữ liệu đi qua những lớp chức năng nào trước khi sẵn sàng cho phân tích?
3. Ai sử dụng dữ liệu và sử dụng để làm gì?

---

## 2. Enterprise Overview

```text
                           +----------------------------------+
                           | ORCHESTRATION & OBSERVABILITY    |
                           | Schedule / Retry / State / Alert |
                           +----------------+-----------------+
                                            |
                                            v
+-------------------+      +----------------------------------------------+
| SOURCE SYSTEMS    |      |              DATA PLATFORM                   |
|                   |      |                                              |
| Operational DB    |----->|  Ingestion  ->  Data Lake  ->  Processing   |
| External APIs     |      |                    |              |           |
| File-based Source |      |                    v              v           |
+-------------------+      |              Analytics Warehouse             |
                           |                    |                          |
                           |                    v                          |
                           |              Business Data Marts              |
                           +--------------------+--------------------------+
                                                |
                                                v
                           +----------------------------------------------+
                           | DATA CONSUMPTION                             |
                           | BI / Reports / Ad-hoc Analytics              |
                           +----------------------------------------------+
```

---

## 3. Các lớp chức năng

### 3.1. Source Systems

Nguồn dữ liệu của FastOrder gồm ba nhóm:

- **Operational database:** dữ liệu giao dịch và trạng thái nghiệp vụ đang thay đổi.
- **External APIs:** dữ liệu ngữ cảnh bên ngoài như thời tiết.
- **File-based sources:** dữ liệu được giao theo file, ví dụ clickstream.

### 3.2. Data Ingestion

Chịu trách nhiệm đưa dữ liệu từ source boundary vào Data Platform một cách có kiểm soát. Lớp này xử lý các vấn đề như incremental extraction, file discovery, API calls, retry, idempotency và ingestion metadata; không thực hiện business cleansing của Silver.

### 3.3. Data Lake

Data Lake là vùng lưu trữ bền vững cho dữ liệu trước và sau bước chuẩn hóa đầu tiên:

- **Landing:** vùng tiếp nhận/prepared source dành cho file-based flow.
- **Bronze:** dữ liệu đã được FastOrder ingestion chấp nhận, giữ fidelity với nguồn và bổ sung technical metadata.
- **Silver:** dữ liệu đã được chuẩn hóa, kiểm tra chất lượng và sẵn sàng cho downstream analytics.

### 3.4. Data Processing

Thực thi transformation giữa các lớp lưu trữ: parse, chuẩn hóa schema, time normalization, reconciliation, data quality và các phép biến đổi có khối lượng lớn.

### 3.5. Analytics Warehouse & Data Marts

Tổ chức dữ liệu theo mô hình phục vụ OLAP và business analytics. Business logic dạng SQL, Fact/Dimension và data marts được xây dựng ở đây thay vì đẩy ngược vào Bronze.

### 3.6. Data Consumption

Cung cấp dữ liệu cho dashboard, báo cáo và phân tích ad-hoc của các stakeholder như CEO, Sales, Logistics, Inventory và Finance.

### 3.7. Orchestration & Observability

Đây là lớp điều phối xuyên suốt, không phải một data zone. Nó quản lý lịch chạy, dependency, retry, trạng thái, logging và khả năng quan sát pipeline.

---

## 4. Nguyên tắc kiến trúc

- **Business-first:** business requirement đi trước tool selection.
- **Thin orchestration:** DAG điều phối; logic tái sử dụng nằm trong application modules.
- **Storage/compute separation:** nơi lưu trữ và nơi tính toán là hai trách nhiệm khác nhau.
- **Replay-safe ingestion:** retry không được tạo thêm logical duplicate cho cùng một ingestion identity.
- **Raw fidelity:** Bronze không âm thầm sửa business data.
- **Fail-fast:** state hoặc data contract bất thường phải làm pipeline thất bại rõ ràng.
- **Evidence-based certification:** Airflow task `success` không đồng nghĩa dữ liệu đã đúng; pipeline phải được reconciliation với source/control state.

---

## 5. Phạm vi của tài liệu

Tài liệu này chỉ mô tả **kiến trúc mức cao**. Xem thêm:

- `02-logical-architecture.md`: trách nhiệm từng logical zone.
- `03-physical-architecture.md`: công nghệ hiện tại và target stack.
- `04-data-flow-diagram.md`: luồng dữ liệu E2E theo từng source type.
- `../07-progress/02-current-status.md`: trạng thái triển khai thực tế tại thời điểm hiện tại.
