# FastOrder Documentation Index

**Cập nhật gần nhất:** 29/07/2026

Tài liệu được chia theo câu hỏi mà mỗi nhóm phải trả lời:

| Nhóm | Câu hỏi chính |
|---|---|
| `01-business` | FastOrder là doanh nghiệp gì và cần giải quyết bài toán nào? |
| `02-architecture` | Nền tảng dữ liệu được thiết kế như thế nào? |
| `03-data` | Nguồn dữ liệu, thực thể và schema là gì? |
| `04-pipelines` | Dữ liệu di chuyển và được xử lý ra sao? |
| `05-operations` | Chạy, reset và kiểm tra hệ thống local như thế nào? |
| `06-decisions` | Vì sao các quyết định kỹ thuật được chọn? |
| `07-progress` | Đã làm gì, đang ở đâu và tiếp theo là gì? |

## Kiểm toán trạng thái tài liệu

| File hiện có | Trạng thái | Hành động |
|---|---|---|
| `01-business/business-context.md` | Ổn định | Giữ nguyên; chỉ sửa khi business scope thay đổi |
| `01-business/stakeholders-kpis.md` | Ổn định | Giữ nguyên |
| `02-architecture/01-high-level-architecture.md` | Ổn định về định hướng | Thêm ghi chú implementation status nếu chưa có |
| `02-architecture/02-logical-architecture.md` | Ổn định | Giữ nguyên |
| `02-architecture/03-physical-architecture.md` | Đúng target architecture nhưng implementation mới ở local | Phân biệt rõ **target** và **current implementation** |
| `02-architecture/04-data-flow-diagram.md` | Cần kiểm tra | Bổ sung bootstrap flow: Olist CSV → PostgreSQL → Simulator → Airflow incremental |
| `03-data/01-source-systems.md` | Cần cập nhật trạng thái | Ghi Olist là dữ liệu seed; PostgreSQL là operational source |
| `03-data/02-business-entities.md` | Cần đối chiếu | Đồng bộ tên bảng với schema hiện tại |
| `03-data/03-data-dictionary.md` | Lạc hậu | Thay bằng Data Dictionary v3 trong gói này |
| `04-pipelines/` | Thiếu | Thêm `01-initial-olist-bootstrap.md` |
| `05-operations/` | Thiếu | Thêm `01-local-development-runbook.md` |
| `06-decisions/` | Thiếu | Thêm `01-decision-log.md` |
| `07-progress/` | Thiếu | Thêm timeline, current status và roadmap |

## Quy tắc chống documentation drift

Sau mỗi thay đổi, cuối phiên làm việc phải ghi rõ một trong hai kết luận:

```text
Docs cần cập nhật:
- <tên file cụ thể>
```

hoặc:

```text
Không cần cập nhật docs.
```

Các thay đổi bắt buộc cập nhật docs:

- Cấu trúc thư mục hoặc tên file.
- Schema, PK, FK, index hoặc naming.
- Source system và API.
- Pipeline, orchestration hoặc lịch chạy.
- Biến môi trường và cách khởi chạy.
- Technology stack.
- Quyết định kỹ thuật quan trọng.
