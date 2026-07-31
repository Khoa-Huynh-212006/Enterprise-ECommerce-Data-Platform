# Initial Olist Bootstrap Pipeline

**Status:** Implemented and successfully executed on 29/07/2026.

## Purpose

Khởi tạo FastOrder operational PostgreSQL từ Olist CSV trước khi Faker simulator bắt đầu tạo biến động.

Đây là **bootstrap pipeline chạy một lần**, không phải batch DAG định kỳ.

## Data flow

```text
data/raw/olist/*.csv
        ↓
pandas.read_csv()
        ↓
SQLAlchemy transaction
        ↓
FastOrder PostgreSQL
```

## Components

| Component | Responsibility |
|---|---|
| `database/schema.sql` | Định nghĩa bảng, PK, FK, indexes |
| `fastorder/db/connection.py` | Cung cấp SQLAlchemy Engine |
| `fastorder/db/init_db.py` | Reset và tạo schema |
| `fastorder/ingestion/load_olist.py` | Nạp Olist vào schema có sẵn |

## Execution order

Thứ tự phải tôn trọng foreign key:

1. `geolocation`
2. `product_category_name_translation`
3. `customers`
4. `products`
5. `sellers`
6. `orders`
7. `order_items`
8. `order_payments`
9. `order_reviews`

`warehouses` và `inventory` không có file Olist tương ứng nên chưa được nạp bởi loader này.

## Transaction strategy

Initial bootstrap dùng một transaction cho toàn bộ 9 bảng:

```text
Tất cả thành công → COMMIT
Một bảng lỗi       → ROLLBACK toàn bộ
```

Lý do:

- Dataset bootstrap tương đối nhỏ.
- Chưa có downstream consumer.
- Ưu tiên snapshot nhất quán.
- Tránh phải xây checkpoint/idempotency framework quá sớm.

Airflow incremental sau này sẽ không dùng một transaction chung cho mọi entity.

## Re-run behavior

Loader dùng `if_exists="append"` và không idempotent.

Muốn chạy lại:

```bash
python -m fastorder.db.init_db
python -m fastorder.ingestion.load_olist
```

Không chạy loader lần hai trên database đã có cùng snapshot nếu chưa reset.

## Data-quality incident: geolocation

### Symptom

`IntegrityError` xảy ra khi load bảng geolocation.

### Root cause

Composite key dựa trên ZIP/latitude/longitude không phải khóa nghiệp vụ đáng tin cậy:

- Source có thể có duplicate.
- `DECIMAL(10,6)` có thể làm các tọa độ gần nhau trở thành cùng giá trị.
- Không có business rule chứng minh bộ ba luôn unique.

### Resolution

- Dùng surrogate identity key cho geolocation.
- Không âm thầm xóa duplicate trong initial loader.
- Giữ dữ liệu source; deduplication business-facing sẽ thực hiện ở Silver nếu cần.

## Success criteria

- 9 source files được nạp.
- Transaction commit.
- Không có partial snapshot.
- Exception được raise lại để shell/CI/Airflow nhận biết thất bại.

## Post-load Reconciliation

- Đã thực hiện kiểm tra số lượng dòng sau khi tải dữ liệu.
- Ghi nhận 9 bảng có row count khớp hoàn toàn với các file CSV nguồn.
- Foreign-key integrity validation thành công: 0 orphan records trong toàn bộ 6 quan hệ được kiểm tra.
- Required NULL validation thành công: 17/17 trường dữ liệu bắt buộc tuân thủ đúng contract.

> **Lưu ý Kiến trúc:** Quá trình validation tại bước này chỉ tập trung vào **Ingestion Fidelity** (dữ liệu vào khớp dữ liệu nguồn) và **Structural Integrity** (Schema, PK, FK, NOT NULL hợp lệ). Không thực hiện làm sạch dữ liệu hay chuẩn hóa nghiệp vụ (Business Data Quality) tại initial loader.

## Full Bootstrap Sequence

Sau khi chạy `docker compose up -d`, đây là thứ tự chạy lệnh đầy đủ để thiết lập toàn bộ môi trường dữ liệu local:

```bash
python -m fastorder.db.init_db
python -m fastorder.ingestion.load_olist
python -m fastorder.ingestion.validate_olist_load
python -m fastorder.db.seed_warehouses
python -m fastorder.db.seed_inventory
python -m fastorder.db.validate_inventory