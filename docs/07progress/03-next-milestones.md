# Next Milestones

**Bắt đầu từ:** sau ngày 29/07/2026

## Milestone 10 — Post-load validation

**Must-have**

- So sánh số dòng giữa từng CSV và bảng PostgreSQL.
- Xác nhận row count geolocation sau khi đổi surrogate key.
- Kiểm tra FK orphan:
  - orders → customers.
  - order_items → orders/products/sellers.
  - payments/reviews → orders.
- Kiểm tra các cột NOT NULL.
- Kiểm tra min/max timestamp.
- Ghi validation summary.

**Definition of Done**

- 9 bảng nguồn được đối chiếu.
- Không có orphan records ngoài ngoại lệ đã giải thích.
- Có file kết quả trong `docs/07-progress/`.

---

## Milestone 11 — Seed warehouses and inventory

**Must-have trước simulator**

- Xác định danh sách warehouse.
- Gán city/state.
- Khởi tạo inventory theo product và warehouse.
- Xác định business rule gán order item vào warehouse.

**Không làm quá mức**

- Chưa cần inventory reservation engine phức tạp.
- Chưa cần tối ưu logistics.
- Chưa cần nhiều loại tồn kho nếu simulator chưa dùng.

---

## Milestone 12 — Faker simulator design

Thiết kế trước khi code:

- Những event nào được tạo?
- Tần suất mỗi event?
- Bảng nào INSERT?
- Bảng nào UPDATE?
- Transaction boundary?
- Cách cập nhật `updated_at`?
- Cách bảo đảm FK?
- Cách tránh số lượng tồn kho âm?

Event tối thiểu:

- Tạo customer.
- Tạo order và order items.
- Tạo payment.
- Chuyển trạng thái order.
- Trừ inventory.
- Tạo review sau delivered.

---

## Milestone 13 — Faker simulator implementation

- Python process riêng, không phải Airflow DAG.
- Dùng SQLAlchemy Core.
- Transaction theo business event.
- Có logging và error handling.
- Có chế độ chạy một lần để test.
- Có chế độ chạy liên tục sau khi ổn định.

---

## Milestone 14 — Airflow incremental PostgreSQL → Bronze

- Mỗi entity là task hoặc task group phù hợp.
- Timestamp-based incremental extraction bằng `updated_at`.
- Watermark riêng theo entity.
- Retry độc lập.
- Không gọi đây là log-based CDC.
- Ghi raw data vào Bronze theo cấu trúc partition đã chốt.

---

## Milestone 15 trở đi

```text
Bronze → Silver bằng PySpark
External APIs và file-based sources
Silver → Analytics storage
dbt Gold models
Power BI dashboards
Data quality, monitoring, CI/CD và documentation polish
```
