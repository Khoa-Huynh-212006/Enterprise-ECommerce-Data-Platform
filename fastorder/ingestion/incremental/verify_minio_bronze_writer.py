import io
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq

from fastorder.ingestion.incremental.bronze_writer import (
    write_bronze_batch,
)
from fastorder.storage.minio_client import (
    get_minio_client,
)


BRONZE_BUCKET = "bronze"


def main() -> None:

    ingested_at = datetime.now()

    extraction_id = (
        "test_minio_writer_"
        + ingested_at.strftime(
            "%Y%m%d%H%M%S%f"
        )
    )

    records = [
        {
            "order_id":
                "TEST_ORDER_001",

            "customer_id":
                "TEST_CUSTOMER_001",

            # Cố tình NULL để kiểm tra
            # type vẫn là string.
            "warehouse_id":
                None,

            "order_status":
                "delivered",

            "order_purchase_timestamp":
                datetime(
                    2026,
                    8,
                    31,
                    10,
                    0,
                    0,
                    123456,
                ),

            "order_approved_at":
                None,

            "order_delivered_carrier_date":
                None,

            "order_delivered_customer_date":
                None,

            "order_estimated_delivery_date":
                None,

            "source_system":
                "minio_writer_test",

            "created_at":
                datetime(
                    2026,
                    8,
                    31,
                    10,
                    0,
                    0,
                    123456,
                ),

            "updated_at":
                datetime(
                    2026,
                    8,
                    31,
                    10,
                    0,
                    0,
                    123456,
                ),
        }
    ]

    client = get_minio_client()

    object_key = None

    try:

        # =========================
        # 1. WRITE
        # =========================

        object_key = write_bronze_batch(
            records=records,
            table_name="orders",
            extraction_id=extraction_id,
            ingested_at=ingested_at,
        )

        print(
            "[PASS] Write object:"
        )

        print(
            object_key
        )


        # =========================
        # 2. DOWNLOAD
        # =========================

        response = client.get_object(
            Bucket=BRONZE_BUCKET,
            Key=object_key,
        )

        parquet_bytes = (
            response["Body"].read()
        )

        assert parquet_bytes


        # =========================
        # 3. READ PARQUET
        # =========================

        parquet_file = pq.ParquetFile(
            io.BytesIO(
                parquet_bytes
            )
        )

        schema = (
            parquet_file.schema_arrow
        )

        table = (
            parquet_file.read()
        )


        # =========================
        # 4. SCHEMA CHECK
        # =========================

        assert (
            schema.field(
                "warehouse_id"
            ).type
            == pa.string()
        )

        assert (
            schema.field(
                "updated_at"
            ).type
            == pa.timestamp("us")
        )

        assert (
            schema.field(
                "_ingested_at"
            ).type
            == pa.timestamp("us")
        )

        assert (
            schema.field(
                "_source_updated_at"
            ).type
            == pa.timestamp("us")
        )


        # =========================
        # 5. DATA CHECK
        # =========================

        rows = table.to_pylist()

        assert len(rows) == 1

        row = rows[0]

        assert (
            row["order_id"]
            == "TEST_ORDER_001"
        )

        assert (
            row["warehouse_id"]
            is None
        )

        assert (
            row["updated_at"].microsecond
            == 123456
        )

        assert (
            row["_ingestion_id"]
            == extraction_id
        )

        assert (
            row["_source_table"]
            == "orders"
        )

        print(
            "[PASS] Parquet schema + data"
        )


        # =========================
        # 6. IDEMPOTENT RETRY
        # =========================

        retry_key = write_bronze_batch(
            records=records,
            table_name="orders",
            extraction_id=extraction_id,
            ingested_at=ingested_at,
        )

        assert retry_key == object_key


        objects = client.list_objects_v2(
            Bucket=BRONZE_BUCKET,
            Prefix=(
                object_key.rsplit(
                    "/",
                    1,
                )[0]
            ),
        )

        parquet_objects = [
            item["Key"]
            for item in objects.get(
                "Contents",
                [],
            )
            if item["Key"].endswith(
                ".parquet"
            )
        ]

        assert parquet_objects == [
            object_key
        ]

        print(
            "[PASS] Retry không tạo duplicate"
        )


        print(
            "\nMINIO BRONZE WRITER: PASS"
        )

    finally:

        # =========================
        # CLEANUP
        # =========================

        if object_key is not None:

            client.delete_object(
                Bucket=BRONZE_BUCKET,
                Key=object_key,
            )

            print(
                "[CLEANUP] Test object deleted"
            )


if __name__ == "__main__":
    main()