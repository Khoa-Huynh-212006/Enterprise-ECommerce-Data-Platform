import io

import pyarrow as pa
import pyarrow.parquet as pq

from fastorder.storage.minio_client import (
    get_minio_client,
)

from fastorder.ingestion.incremental.table_config import (
    get_table_config,
)


BRONZE_BUCKET = "bronze"
TABLE_NAME = "orders"


def main() -> None:

    client = get_minio_client()

    config = get_table_config(
        TABLE_NAME
    )


    # Các cột source được contract yêu cầu
    # phải có timestamp precision = microseconds.
    expected_timestamp_columns = {
        column
        for column, configured_type
        in config.column_types.items()
        if configured_type == "timestamp_us"
    }

    # Metadata do Bronze writer tạo.
    expected_timestamp_columns.update(
        {
            "_ingested_at",
            "_source_updated_at",
        }
    )


    # =========================
    # 1. DISCOVER OBJECTS
    # =========================

    object_keys = []

    continuation_token = None

    while True:

        request = {
            "Bucket": BRONZE_BUCKET,
            "Prefix": f"{TABLE_NAME}/",
        }

        if continuation_token:
            request["ContinuationToken"] = (
                continuation_token
            )

        response = client.list_objects_v2(
            **request
        )

        for item in response.get(
            "Contents",
            [],
        ):

            key = item["Key"]

            if not key.endswith(
                "/part-000.parquet"
            ):
                continue

            if (
                "/extraction_id=test_"
                in key
            ):
                continue

            object_keys.append(
                key
            )

        if not response.get(
            "IsTruncated",
            False,
        ):
            break

        continuation_token = (
            response[
                "NextContinuationToken"
            ]
        )


    assert object_keys, (
        "Không tìm thấy production "
        "Orders Parquet."
    )

    print(
        "Production Parquet files:",
        len(object_keys),
    )


    # =========================
    # 2. VERIFY EVERY FILE
    # =========================

    for index, key in enumerate(
        object_keys,
        start=1,
    ):

        response = client.get_object(
            Bucket=BRONZE_BUCKET,
            Key=key,
        )

        raw = response["Body"].read()

        assert raw, (
            f"Object rỗng: {key}"
        )


        parquet_file = pq.ParquetFile(
            io.BytesIO(raw)
        )

        arrow_schema = (
            parquet_file.schema_arrow
        )

        physical_schema = str(
            parquet_file.schema
        )


        # =========================
        # 3. TIMESTAMP CONTRACT
        # =========================

        for column in (
            expected_timestamp_columns
        ):

            actual_type = (
                arrow_schema
                .field(column)
                .type
            )

            assert (
                actual_type
                == pa.timestamp("us")
            ), (
                f"{key}: "
                f"{column} có type "
                f"{actual_type}, "
                "expected timestamp[us]."
            )


        # Defense thêm ở physical
        # Parquet representation.
        assert (
            "NANOS"
            not in physical_schema.upper()
        ), (
            f"{key}: phát hiện "
            "TIMESTAMP(NANOS)."
        )


        # warehouse_id từng bị
        # inferred thành null type.
        warehouse_type = (
            arrow_schema
            .field(
                "warehouse_id"
            )
            .type
        )

        assert (
            warehouse_type
            == pa.string()
        ), (
            f"{key}: warehouse_id "
            f"có type {warehouse_type}, "
            "expected string."
        )


        print(
            f"[PASS {index:02d}/"
            f"{len(object_keys):02d}] "
            f"{key}"
        )


    print(
        "\nORDERS PRODUCTION PARQUET "
        "SCHEMA: PERFECT PASS"
    )


if __name__ == "__main__":
    main()