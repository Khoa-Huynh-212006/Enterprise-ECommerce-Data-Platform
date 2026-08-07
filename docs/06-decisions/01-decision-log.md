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

---

## D-012 — Separation of Ingestion Validation and Business Data Quality

**Date:** 30/07/2026  
**Decision:** Các quy tắc kiểm tra chất lượng dữ liệu nghiệp vụ (business data-quality rules) sẽ được thực hiện ở lớp Transformation (Bronze → Silver), không nằm trong initial loader.

**Reason:** 
- Initial loader chỉ chịu trách nhiệm cho Ingestion Fidelity và Structural Integrity (giữ nguyên bản dữ liệu thô, không fill NULL, không xóa bản ghi lỗi).
- Bảo vệ dữ liệu gốc tại lớp Bronze để đảm bảo Data Lineage và phục vụ Root Cause Analysis về sau. Pipeline nạp dữ liệu cũng nhờ đó mà nhẹ nhàng và dễ bảo trì hơn.
---

## D-013 — Synthetic Sparse Inventory Strategy

**Date:** 30/07/2026  
**Decision:** Sử dụng chiến lược phân bổ tồn kho thưa (Sparse Inventory) và sinh số lượng ngẫu nhiên có kiểm soát (Fixed Random Seed).  
**Quy tắc phân bổ:**
- Mỗi sản phẩm có mặt tại đúng 1 kho chính (Main warehouse: WH_HN hoặc WH_HCM) với số lượng 50–200.
- Mỗi sản phẩm có mặt tại đúng 1 kho vùng (Regional warehouse: WH_HP, WH_DN, hoặc WH_CT) với số lượng 20–100.
- 20% xác suất sản phẩm có mặt ở kho thứ ba (trong số các kho còn lại) với số lượng 10–50.

**Reason:** 
- Tránh việc phân bổ tất cả sản phẩm vào tất cả các kho (Dense Inventory), gây ra sự thiếu thực tế về mặt nghiệp vụ logistics và làm phình to database không cần thiết (chỉ tạo ~72.000 records thay vì ~164.000 records).
- Việc dùng Fixed Seed (`random.seed(42)`) đảm bảo tính tái lập (reproducibility) khi reset DB, hỗ trợ tốt nhất cho quá trình debug, test và data validation.

```markdown
---

## D-013 — Synthetic Sparse Inventory Strategy

**Date:** 30/07/2026  
**Decision:** 
- Áp dụng chiến lược Sparse Inventory, phân bổ mỗi sản phẩm có mặt tại 2–3 warehouse.
- Sử dụng Fixed Seed (`random.seed(42)`).
- Sử dụng mệnh đề `ON CONFLICT DO NOTHING` cho quá trình seed inventory.

**Reason:** 
- Tạo ra phân phối dữ liệu thực tế hơn so với việc seed tất cả sản phẩm vào tất cả kho.
- Fixed seed đảm bảo tính nhất quán (reproducible) của dữ liệu mô phỏng qua các lần thiết lập môi trường.
- `ON CONFLICT DO NOTHING` là chốt chặn an toàn cốt lõi: nó ngăn việc ghi đè trạng thái vận hành (operational inventory) nếu script seed vô tình bị chạy lại trong lúc hệ thống đang có dữ liệu giao dịch động.

---

## D-014 — Faker Simulator: State Machine & Quantity Logic

**Date:** 30/07/2026  
**Decision:** 
1. **Event CREATE_ORDER:** Khởi tạo order với status mặc định là `created`. Payment được sinh đồng thời trong cùng một transaction cùng với việc trừ số lượng (`quantity`) vào bảng `inventory`.
2. **Event ADVANCE_ORDER_STATUS:** Không random trạng thái lộn xộn, mà chỉ tịnh tiến (UPDATE) ngẫu nhiên theo đúng luồng State Machine (ví dụ: `created` -> `approved` -> `processing` -> `shipped` -> `delivered`).
3. **Item Quantity:** Số lượng từng item được chọn ngẫu nhiên trong khoảng `[1, quantity_available]`.

**Reason:** 
- Đảm bảo tính toàn vẹn dữ liệu: Order sinh ra là phải thu tiền và trừ kho ngay lập tức, nếu lỗi 1 bước sẽ Rollback toàn bộ.
- Tuân thủ State Machine giúp Pipeline Airflow Incremental sau này có cơ hội "bắt" (capture) được các sự kiện UPDATE thay đổi trạng thái theo thời gian thực (CDC mô phỏng).

---

## D-015 — Faker Simulator: Concurrency Control & B2C Limit

**Date:** 30/07/2026  
**Decision:** 
1. **Quantity Cap:** Giới hạn số lượng mua tối đa cho mỗi mặt hàng trong một đơn là 10 (hoặc bằng tồn kho hiện tại nếu tồn kho < 10) để phản ánh đúng hành vi mua sắm B2C.
2. **Concurrency Handling:** Không sử dụng `executemany` cho lệnh `UPDATE inventory` vì driver `psycopg2` không cam kết trả về `rowcount` chính xác trong chế độ này. Thay vào đó, Simulator duyệt vòng lặp thực thi từng lệnh `UPDATE` kèm điều kiện `quantity_available >= :buy_qty` và kiểm tra `rowcount == 1`. 

**Reason:** 
- Đảm bảo tính chân thực của dữ liệu mô phỏng.
- Kỹ thuật Optimistic Locking (kiểm tra rowcount) khóa chặt lỗ hổng Overselling khi chạy nhiều tiến trình Simulator song song, nếu có tranh chấp tài nguyên (race condition), transaction sẽ lập tức Rollback.

## D-016 — Architecture of Simulator Runner

**Date:** 01/08/2026  
**Decision:** 
- Triển khai Simulator Runner hỗ trợ chạy hữu hạn (Bounded) hoặc liên tục (Continuous).
- Mỗi cycle tạo 0-3 orders và gọi status updater.
- Áp dụng nguyên tắc Fail-fast, không che giấu exception.
- Hỗ trợ graceful shutdown bằng Ctrl+C.
- Chạy independent validator ở cuối phiên.

**Reason:** 
- Bounded mode bảo đảm có test data phục vụ quá trình test nhanh, trong khi Continuous mode mô phỏng operational traffic dài hạn.
- Nguyên tắc Fail-fast giúp phát hiện ngay lỗi Database/Logic thay vì chạy lặp vô hạn. Graceful shutdown ngăn chặn treo transaction.
- Independent validator chốt chặn chất lượng dữ liệu cuối phiên đảm bảo dữ liệu sinh ra không vi phạm Data Invariants (12/12 rules PASS).

## D-017 — At-Least-Once Bronze Ingestion & Timestamp-based Extraction

**Date:** 05/08/2026
**Decision:**
*   Sử dụng Timestamp-based Incremental Extraction thay vì WAL-based CDC (Debezium/Kafka) cho giai đoạn MVP.
*   Chốt Watermark theo cơ chế Composite Key: `(updated_at, order_id)`.
*   Chấp nhận Delivery Semantics là **At-least-once** từ PostgreSQL xuống Bronze. 
*   Việc khử trùng lặp (Deduplication) sẽ được thực hiện ở lớp Silver.

**Reason:**
*   Hệ thống file local không hỗ trợ distributed transaction. Việc giả lập Exactly-once delivery ở bước này là quá rườm rà và dễ phát sinh lỗi (error-prone). 
*   Medallion Architecture sinh ra là để các lớp san sẻ gánh nặng cho nhau. Bronze có nhiệm vụ lấy dữ liệu nhanh nhất và an toàn nhất (append-only), còn Silver xử lý logic nghiệp vụ (Deduplicate).
*   Upper Watermark giúp cô lập batch dữ liệu, tránh việc query đuổi theo dữ liệu do Simulator sinh ra liên tục.

## D-018 — Timestamp Honesty & Schema Enforcement for Incremental Extraction

**Date:** 05/08/2026
**Decision:**
*   Bảo tồn định dạng `TIMESTAMP WITHOUT TIME ZONE` cho checkpoint JSON (dạng naive ISO 8601, không gắn cờ `Z` UTC).
*   Thực thi cứng ràng buộc `NOT NULL` cho cột `updated_at` trong database.
*   Thiết lập Composite Index `(updated_at, order_id)` trực tiếp trên schema.

**Reason:**
*   Ép múi giờ UTC trong code Python đối với một database không lưu múi giờ sẽ tạo ra metadata giả, gây lỗi lệch pha hệ thống (offset drift) về sau. Phải trung thực với schema nguồn.
*   Giá trị `updated_at = NULL` sẽ vĩnh viễn lọt lưới Incremental Query, nên `NOT NULL` là lá chắn bắt buộc.
*   Composite Index là thành phần vật lý không thể thiếu để duy trì hiệu năng khi query quét theo watermark ngày càng phình to.

## D-019 — Idempotent Overwrite for Bronze Layer (Extraction ID Reuse)

**Date:** 06/08/2026
**Decision:**
*   Lựa chọn Phương án B (Idempotent Semantics) cho kiến trúc ghi file tại tầng Bronze. 
*   Quá trình retry (chạy lại do lỗi) sẽ dùng lại cùng một `extraction_id` cho cùng một lô công việc.
*   Thực hiện ghi đè an toàn thông qua hàm `os.replace()` (Atomic Replace).

**Reason:**
*   Ngăn chặn sự tích tụ của các thư mục/file Parquet rác trong Data Lake khi tiến trình crash giữa bước "Ghi Parquet" và "Commit Checkpoint". 
*   Dù tầng Silver có năng lực Deduplicate, việc giữ sạch tầng vật lý Bronze ngay từ đầu (giảm thiểu số lượng file trùng lặp) là tiêu chuẩn công nghiệp tốt nhất, giúp giảm tải IO và tránh phải xây dựng các kịch bản dọn rác (Garbage Collection/Vacuum) phức tạp.

## D-020 — Use Pending Batch Context for Stable Crash Recovery

**Date:** 07/08/2026
**Decision:**
* Bổ sung cơ chế `Pending Batch Context` lưu dưới dạng JSON atomic để theo dõi trạng thái của batch đang chạy dở.
* Kế thừa chặt chẽ `run_id`, `ingested_at`, `batch_size`, và ranh giới watermark (lower/batch_upper) từ Pending Context khi tiến hành phục hồi sau sự cố.

**Reason:**
* Checkpoint chỉ trả lời câu hỏi "Pipeline đã hoàn thành đến đâu?", nhưng không biết "Tiến trình đang làm dở việc gì?". 
* Việc không có Pending Context sẽ khiến tiến trình khi restart bị mất `extraction_id` cũ, tự động sinh ID mới và ghi file Parquet mới, dẫn đến rác dữ liệu trên Data Lake hoặc ghi đè sai batch. Pending Context đảm bảo ranh giới dữ liệu và danh tính của lần chạy (Stable Run Identity) được bảo toàn tuyệt đối xuyên suốt các lần khởi động lại tiến trình.

## D-021 — Áp dụng Stable Retry Context và Fail-Fast cho State Management

**Date:** 2026-08-07
**Decision:**
* Sử dụng `Pending Batch Context` để kế thừa `run_id`, `ingested_at`, `batch_size`, và watermark boundaries khi phục hồi sau sự cố.
* Tuyệt đối không dùng code để tự động lấp liếm (ví dụ: tự xóa file checkpoint rỗng hoặc tự đoán extraction_id). Hệ thống phải Fail-fast khi phát hiện trạng thái state file bất thường.

**Reason:**
* Đảm bảo ranh giới dữ liệu và danh tính của lần chạy (Stable Run Identity) không bị biến đổi xuyên suốt các lần restart.
* Việc tự động bỏ qua lỗi của file trạng thái (như file bị rỗng do I/O error) có thể dẫn đến hậu quả nghiêm trọng như kéo lại toàn bộ lịch sử dữ liệu (Disaster Risk) hoặc ghi đè sai batch. Con người phải can thiệp khi State files bị hỏng.