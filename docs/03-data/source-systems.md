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