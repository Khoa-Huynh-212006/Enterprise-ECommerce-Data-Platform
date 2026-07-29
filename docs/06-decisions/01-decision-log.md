# Technical Decision Log

## D-001 — Business-first platform design

**Date:** 24–27/07/2026  
**Decision:** Bắt đầu từ FastOrder business context và KPI, không bắt đầu từ tool.

**Reason:** Tránh project trở thành demo công nghệ không có business narrative.

---

## D-002 — Olist is seed data, PostgreSQL is the operational source

**Date:** 24–28/07/2026  
**Decision:** Olist CSV chỉ khởi tạo snapshot; Faker simulator sẽ tiếp tục INSERT/UPDATE PostgreSQL.

**Reason:** Tạo nguồn OLTP có biến động để học incremental ingestion đúng ngữ cảnh.

---

## D-003 — Schema-first instead of Pandas-managed schema

**Date:** 29/07/2026  
**Decision:** Dùng `database/schema.sql`; không dùng `to_sql(if_exists="replace")` để Pandas tự tạo bảng.

**Reason:** Kiểm soát data types, constraints, naming, FK và indexes.

---

## D-004 — SQLAlchemy Core with psycopg2

**Date:** 29/07/2026  
**Decision:** Dùng SQLAlchemy Core Engine, giữ psycopg2 làm PostgreSQL driver.

**Reason:**

- Học Engine, Connection và Transaction.
- Dùng chung cho loader và simulator.
- Không cần ORM cho pipeline data engineering.

---

## D-005 — Separate SQL artifacts and Python database code

**Date:** 29/07/2026  
**Decision:**

```text
database/      → schema.sql và SQL artifacts
fastorder/db/  → connection.py, init_db.py
```

**Reason:** Trách nhiệm rõ ràng và tránh `database/` trùng nghĩa.

---

## D-006 — Thin Airflow DAGs

**Date:** 29/07/2026  
**Decision:** Logic tái sử dụng nằm trong `fastorder/`; DAG chỉ orchestration.

**Reason:** Dễ test, dễ tái sử dụng, tránh DAG file phình to.

---

## D-007 — No `is_simulated` or `simulation_id`

**Date:** 29/07/2026  
**Decision:** Không đánh dấu record simulator trong operational tables.

**Reason:** Simulator được xem là producer của operational data; downstream không cần phân biệt seed và simulated record.

---

## D-008 — Atomic initial bootstrap

**Date:** 29/07/2026  
**Decision:** Initial Olist loader dùng một transaction cho 9 bảng.

**Reason:** Đảm bảo snapshot nhất quán và tránh partial initial state.

**Boundary:** Airflow incremental sau này sẽ retry/checkpoint theo entity, không dùng một transaction khổng lồ.

---

## D-009 — Surrogate primary key for geolocation

**Date:** 29/07/2026  
**Decision:** Không dùng ZIP/lat/lng làm composite primary key.

**Reason:** Source duplicate và precision rounding có thể vi phạm uniqueness; không có business rule xác nhận natural key.

---

## D-010 — Timestamp-based incremental extraction

**Date:** 29/07/2026  
**Decision:** Dự kiến Airflow đọc thay đổi bằng `updated_at`.

**Terminology:** Đây là timestamp-based incremental extraction, không phải log-based CDC.

**Implementation note:** UPDATE phải cập nhật `updated_at` rõ ràng hoặc dùng trigger.

---

## D-011 — Documentation must evolve with implementation

**Date:** 29/07/2026  
**Decision:** Mỗi thay đổi phải kết thúc bằng “Docs cần cập nhật” hoặc “Không cần cập nhật docs”.

**Reason:** Ngăn architecture, schema và runbook trở nên lạc hậu.
