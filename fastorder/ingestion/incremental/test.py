from datetime import datetime
from fastorder.ingestion.incremental.adls_bronze_writer import (
    _build_arrow_table,
)

import pyarrow as pa
import pyarrow.parquet as pq
import io

test_records = [
    {
        "order_id": "O001",
        "customer_id": "C001",

        # Cố tình NULL để test
        # schema không bị suy luận thành null type.
        "warehouse_id": None,

        "order_status": "delivered",

        "order_purchase_timestamp":
            datetime(
                2026,
                8,
                30,
                10,
                0,
                0,
                123456,
            ),

        "order_approved_at": None,
        "order_delivered_carrier_date": None,
        "order_delivered_customer_date": None,
        "order_estimated_delivery_date": None,

        "source_system": "olist_seed",

        "created_at":
            datetime(
                2026,
                8,
                30,
                10,
                0,
                0,
                123456,
            ),

        "updated_at":
            datetime(
                2026,
                8,
                30,
                10,
                0,
                0,
                123456,
            ),
    },
]

table = _build_arrow_table(
    records=test_records,
    table_name="orders",
    extraction_id="test_schema",
    ingested_at=datetime(
        2026,
        8,
        30,
        12,
        0,
        0,
        123456,
    ),
)

buffer = io.BytesIO()

pq.write_table(
    table,
    buffer,
)

buffer.seek(0)

parquet_file = pq.ParquetFile(
    buffer
)

parquet_schema = (
    parquet_file.schema_arrow
)

print(
    "\n=== PARQUET ARROW SCHEMA ==="
)

print(
    parquet_schema
)

print(
    "\n=== PARQUET PHYSICAL SCHEMA ==="
)

print(
    parquet_file.schema
)
assert (
    parquet_schema.field(
        "warehouse_id"
    ).type
    == pa.string()
)

assert (
    parquet_schema.field(
        "updated_at"
    ).type
    == pa.timestamp("us")
)

assert (
    parquet_schema.field(
        "_ingested_at"
    ).type
    == pa.timestamp("us")
)

assert (
    parquet_schema.field(
        "_source_updated_at"
    ).type
    == pa.timestamp("us")
)

print(
    "\nPARQUET SERIALIZATION: PASS"
)