# Weather History Silver Pipeline

## Mục tiêu

Chuyển Open-Meteo Historical Forecast Bronze thành canonical hourly weather dataset
cho từng warehouse.

## Business Grain

`warehouse_id + weather_time`

## Pipeline Flow

`Committed Bronze`
→ `Processed Control Lookup`
→ `Pending Selection`
→ `Bulk Load`
→ `Raw Historical Candidate`
→ `Raw Data Quality`
→ `Ingestion Validation`
→ `Overlap Reconciliation`
→ `Resolved Candidate`
→ `Final Data Quality`
→ `Existing Silver Conflict Check`
→ `Delta MERGE`
→ `Mark Ingestion Processed`
→ `Persistence Verification`

## Incremental State

Pending:

`Committed Bronze - Processed Control`

Control dataset:

`weather_history_processed_ingestions`

## Overlap Policy

Nếu cùng business key:

- weather values khác nhau → FAIL
- weather values giống nhau → canonicalize

Canonical selection:

`retrieved_at DESC`

Tie-break:

`ingestion_id DESC`

## Successful Outcomes

`SUCCESS`

Có pending ingestion và batch được xử lý hoàn chỉnh.

`NO_OP`

Không có pending ingestion và Silver đã up to date.

## Failure Safety

Canonical Silver được persist trước control state.

Nếu canonical write fail:

→ ingestion không được đánh dấu processed.

Nếu control write fail:

→ ingestion vẫn pending và có thể retry an toàn.