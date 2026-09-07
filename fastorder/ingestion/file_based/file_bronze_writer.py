from datetime import datetime
from io import BytesIO
from zoneinfo import ZoneInfo

import pandas as pd
from botocore.client import BaseClient

from fastorder.ingestion.file_based.file_discovery import (
    DiscoveredFile,
)


SOURCE_NAME = "yoochoose_clickstream"
INGESTION_METHOD = "file_incremental"

LANDING_BUCKET = "landing"
BRONZE_BUCKET = "bronze"

BRONZE_ROOT = "clickstream/yoochoose"


def write_file_to_bronze(
    *,
    minio_client: BaseClient,
    source_root: str,
    source_file: DiscoveredFile,
    ingestion_id: str,
    ingested_at: datetime,
) -> str:

    normalized_root = source_root.strip("/")

    if not normalized_root:
        raise ValueError(
            "source_root không được để trống"
        )

    source_path = (
        f"{normalized_root}/"
        f"{source_file.relative_path}"
    )


    # 1. READ FROM LANDING


    response = minio_client.get_object(
        Bucket=LANDING_BUCKET,
        Key=source_path,
    )

    body = response["Body"]

    try:
        source_bytes = body.read()
    finally:
        body.close()

    downloaded_size = len(source_bytes)

    if downloaded_size != source_file.size:
        raise ValueError(
            "Downloaded source có kích thước "
            "không khớp: "
            f"path={source_file.relative_path}, "
            f"expected={source_file.size}, "
            f"actual={downloaded_size}"
        )


    # 2. PARSE SOURCE CSV


    source_df = pd.read_csv(
        BytesIO(source_bytes),
        header=None,
        dtype=str,
        keep_default_na=False,
        na_filter=False,
        on_bad_lines="error",
    )

    expected_source_columns = 4

    if (
        source_df.shape[1]
        != expected_source_columns
    ):
        raise ValueError(
            "Số lượng cột không đúng với nguồn: "
            f"path={source_file.relative_path}, "
            f"expected={expected_source_columns}, "
            f"actual={source_df.shape[1]}"
        )

    source_df.columns = [
        "session_id",
        "event_timestamp",
        "item_id",
        "category",
    ]

    # 3. ADD BRONZE METADATA

    bronze_df = source_df.assign(
        _source_name=SOURCE_NAME,
        _source_file_path=(
            source_file.relative_path
        ),
        _source_file_etag=source_file.etag,
        _source_file_last_modified=(
            source_file.last_modified
        ),
        _source_file_size=source_file.size,
        _ingestion_id=ingestion_id,
        _ingested_at=ingested_at,
        _ingestion_method=INGESTION_METHOD,
    )

    if len(bronze_df) != len(source_df):
        raise ValueError(
            "Số dòng dữ liệu bị thay đổi "
            "sau khi thêm metadata"
        )

    # 4. SERIALIZE PARQUET

    parquet_buffer = BytesIO()

    bronze_df.to_parquet(
        parquet_buffer,
        engine="pyarrow",
        index=False,
        compression="snappy",
    )

    parquet_bytes = (
        parquet_buffer.getvalue()
    )

    if not parquet_bytes:
        raise ValueError(
            "Parquet serialization tạo ra "
            "kết quả empty"
        )

    if not parquet_bytes.startswith(b"PAR1"):
        raise ValueError(
            "Serialized output không giống "
            "định dạng Parquet"
        )

    # 5. BUILD DETERMINISTIC KEY

    if not ingestion_id.strip():
        raise ValueError(
            "ingestion_id không được để trống"
        )

    if (
        "/" in ingestion_id
        or "\\" in ingestion_id
    ):
        raise ValueError(
            "ingestion_id không hợp lệ: "
            f"{ingestion_id}"
        )

    ingestion_date = (
        ingested_at
        .astimezone(
            ZoneInfo("Asia/Ho_Chi_Minh")
        )
        .date()
        .isoformat()
    )

    bronze_path = (
        f"{BRONZE_ROOT}/"
        f"ingestion_date={ingestion_date}/"
        f"ingestion_id={ingestion_id}/"
        "part-000.parquet"
    )

    # 6. WRITE TO MINIO BRONZE

    minio_client.put_object(
        Bucket=BRONZE_BUCKET,
        Key=bronze_path,
        Body=parquet_bytes,
        ContentType="application/octet-stream",
    )

    # 7. VERIFY WRITE

    metadata = minio_client.head_object(
        Bucket=BRONZE_BUCKET,
        Key=bronze_path,
    )

    remote_size = metadata[
        "ContentLength"
    ]

    if remote_size != len(parquet_bytes):
        raise RuntimeError(
            "Bronze upload verification failed: "
            f"path={bronze_path}, "
            f"expected={len(parquet_bytes)}, "
            f"actual={remote_size}"
        )

    return bronze_path