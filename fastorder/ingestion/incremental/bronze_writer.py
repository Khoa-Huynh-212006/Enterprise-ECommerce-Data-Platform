import io
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq

from fastorder.ingestion.incremental.table_config import (
    get_table_config,
)
from fastorder.storage.minio_client import (
    get_minio_client,
)


BRONZE_BUCKET = "bronze"


def _to_arrow_type(
    configured_type: str,
) -> pa.DataType:

    type_mapping = {
        "string":
            pa.string(),

        "int32":
            pa.int32(),

        "int64":
            pa.int64(),

        "decimal_10_2":
            pa.decimal128(
                10,
                2,
            ),

        "decimal_10_6":
            pa.decimal128(
                10,
                6,
            ),

        "timestamp_us":
            pa.timestamp(
                "us"
            ),
    }

    try:
        return type_mapping[
            configured_type
        ]

    except KeyError as error:
        raise ValueError(
            "Không hỗ trợ kiểu dữ liệu "
            f"'{configured_type}'."
        ) from error


def _build_arrow_schema(
    table_name: str,
) -> pa.Schema:

    config = get_table_config(
        table_name
    )

    fields = []

    for column in config.select_columns:

        configured_type = (
            config.column_types[
                column
            ]
        )

        fields.append(
            pa.field(
                column,
                _to_arrow_type(
                    configured_type
                ),
                nullable=True,
            )
        )

    fields.extend(
        [
            pa.field(
                "_ingestion_id",
                pa.string(),
                nullable=False,
            ),

            pa.field(
                "_ingested_at",
                pa.timestamp("us"),
                nullable=False,
            ),

            pa.field(
                "_source_table",
                pa.string(),
                nullable=False,
            ),

            pa.field(
                "_source_updated_at",
                pa.timestamp("us"),
                nullable=False,
            ),

            pa.field(
                "_ingestion_method",
                pa.string(),
                nullable=False,
            ),
        ]
    )

    return pa.schema(fields)


def _build_arrow_table(
    records: list[dict],
    table_name: str,
    extraction_id: str,
    ingested_at: datetime,
) -> pa.Table:

    schema = _build_arrow_schema(
        table_name
    )

    enriched_records = []

    for record in records:

        enriched_record = {
            **record,

            "_ingestion_id":
                extraction_id,

            "_ingested_at":
                ingested_at,

            "_source_table":
                table_name,

            "_source_updated_at":
                record["updated_at"],

            "_ingestion_method":
                "timestamp_incremental",
        }

        enriched_records.append(
            enriched_record
        )

    return pa.Table.from_pylist(
        enriched_records,
        schema=schema,
    )


def write_bronze_batch(
    records: list[dict],
    table_name: str,
    extraction_id: str,
    ingested_at: datetime,
) -> str:

    if not records:
        raise ValueError(
            "Danh sách records không được rỗng."
        )

    if not table_name:
        raise ValueError(
            "table_name không được để trống."
        )

    if not extraction_id:
        raise ValueError(
            "extraction_id không được để trống."
        )

    if not isinstance(
        ingested_at,
        datetime,
    ):
        raise ValueError(
            "ingested_at phải là datetime."
        )

    if any(
        "updated_at" not in record
        for record in records
    ):
        raise ValueError(
            "records thiếu cột 'updated_at'."
        )


    arrow_table = _build_arrow_table(
        records=records,
        table_name=table_name,
        extraction_id=extraction_id,
        ingested_at=ingested_at,
    )


    buffer = io.BytesIO()

    pq.write_table(
        arrow_table,
        buffer,
    )

    parquet_bytes = (
        buffer.getvalue()
    )

    if not parquet_bytes:
        raise RuntimeError(
            "Parquet serialization tạo ra "
            "payload rỗng."
        )


    ingestion_date = (
        ingested_at.strftime(
            "%Y-%m-%d"
        )
    )

    object_key = (
        f"{table_name}/"
        f"ingestion_date={ingestion_date}/"
        f"extraction_id={extraction_id}/"
        "part-000.parquet"
    )


    minio_client = (
        get_minio_client()
    )

    minio_client.put_object(
        Bucket=BRONZE_BUCKET,
        Key=object_key,
        Body=parquet_bytes,
        ContentType=(
            "application/octet-stream"
        ),
    )


    properties = (
        minio_client.head_object(
            Bucket=BRONZE_BUCKET,
            Key=object_key,
        )
    )

    remote_size = (
        properties["ContentLength"]
    )

    if remote_size != len(
        parquet_bytes
    ):
        raise RuntimeError(
            "Kích thước object trên MinIO "
            "không khớp payload đã serialize. "
            f"Local={len(parquet_bytes)}, "
            f"MinIO={remote_size}"
        )


    return object_key