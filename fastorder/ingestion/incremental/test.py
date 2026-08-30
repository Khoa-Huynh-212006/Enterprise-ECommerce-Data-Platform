from datetime import datetime

from fastorder.ingestion.incremental.adls_bronze_writer import (
    write_adls_bronze_batch,
)


test_time = datetime.now()

test_extraction_id = (
    "test_spark_compatibility_"
    + test_time.strftime(
        "%Y%m%d%H%M%S%f"
    )
)


test_records = [
    {
        "order_id": "TEST_SPARK_001",
        "customer_id": "TEST_CUSTOMER_001",

        # Cố tình NULL để kiểm tra
        # schema vẫn phải là string.
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
    }
]


output_path = (
    write_adls_bronze_batch(
        records=test_records,
        table_name="orders",
        extraction_id=
            test_extraction_id,
        ingested_at=test_time,
    )
)


print(
    "\nTEST FILE WRITTEN:"
)

print(
    output_path
)