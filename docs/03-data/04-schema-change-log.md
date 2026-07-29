# Schema Change Log

## v1 — Early draft

**Date:** 29/07/2026

Characteristics:

- Table names prefixed with `olist_*`.
- Included `is_simulated` and `simulation_id`.
- Added warehouse and inventory concepts.
- Some FK/index/timestamp gaps.

## v2 — FastOrder naming and constraints

**Date:** 29/07/2026

Changes:

- Renamed `olist_*` tables to FastOrder business table names.
- Removed simulator marker columns.
- Added warehouses and inventory relationships.
- Inventory changed to composite PK `(warehouse_id, product_id)`.
- Added NOT NULL constraints.
- Added FK and `updated_at` indexes.
- Added timestamps to inventory.
- Corrected table creation order.

## v3 — Geolocation key correction

**Date:** 29/07/2026

Incident:

- Initial load failed on geolocation with integrity violation.

Root cause:

- Composite PK `(zip_code_prefix, lat, lng)` was not a reliable key.
- Duplicate source rows and six-decimal precision could collide.

Change:

- Added surrogate identity PK `geolocation_id`.
- Removed composite PK.
- Removed geolocation deduplication from loader.
- Preserved source records.

Result:

- Initial Olist load completed successfully.
