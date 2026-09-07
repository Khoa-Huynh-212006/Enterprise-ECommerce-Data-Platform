from datetime import datetime, timezone
from io import BytesIO
from uuid import uuid4

import pandas as pd

from fastorder.storage.minio_client import (
    get_minio_client,
)

from fastorder.ingestion.file_based.file_discovery import (
    discover_files,
)

from fastorder.ingestion.file_based.file_bronze_writer import (
    BRONZE_BUCKET,
    BRONZE_ROOT,
    LANDING_BUCKET,
    write_file_to_bronze,
)


SOURCE_ROOT = (
    "clickstream/yoochoose/prepared"
)

TEST_INGESTION_ID = (
    "test-file-bronze-writer-"
    f"{uuid4().hex[:8]}"
)

TEST_INGESTED_AT = datetime(
    2026,
    8,
    13,
    15,
    0,
    0,
    tzinfo=timezone.utc,
)


def list_objects_under_prefix(
    minio_client,
    prefix: str,
) -> set[str]:

    response = minio_client.list_objects_v2(
        Bucket=BRONZE_BUCKET,
        Prefix=prefix,
    )

    return {
        item["Key"]
        for item in response.get(
            "Contents",
            [],
        )
    }


def cleanup_prefix(
    minio_client,
    prefix: str,
) -> None:

    object_keys = list_objects_under_prefix(
        minio_client,
        prefix,
    )

    for object_key in object_keys:

        minio_client.delete_object(
            Bucket=BRONZE_BUCKET,
            Key=object_key,
        )

        print(
            f"[CLEANUP] Deleted: "
            f"{object_key}"
        )


minio_client = get_minio_client()



# 1. DISCOVER SOURCE


files = discover_files(
    minio_client=minio_client,
    root_path=SOURCE_ROOT,
)

if not files:
    raise RuntimeError(
        "Không có YOOCHOOSE prepared "
        "CSV files discovered"
    )


source_file = files[0]

print(
    "Đang kiểm tra source file:",
    source_file.relative_path,
)


source_path = (
    f"{SOURCE_ROOT}/"
    f"{source_file.relative_path}"
)



# 2. READ SOURCE FROM MINIO LANDING


response = minio_client.get_object(
    Bucket=LANDING_BUCKET,
    Key=source_path,
)

body = response["Body"]

try:
    source_bytes = body.read()
finally:
    body.close()


expected_df = pd.read_csv(
    BytesIO(source_bytes),
    header=None,
    dtype=str,
    keep_default_na=False,
    na_filter=False,
    on_bad_lines="error",
)


if expected_df.shape[1] != 4:
    raise AssertionError(
        "Số cột nguồn không mong đợi: "
        f"{expected_df.shape[1]}"
    )


expected_df.columns = [
    "session_id",
    "event_timestamp",
    "item_id",
    "category",
]


print(
    f"Source rows: "
    f"{len(expected_df):,}"
)



# 3. EXPECTED BRONZE PREFIX


ingestion_date = (
    TEST_INGESTED_AT
    .astimezone(
        __import__("zoneinfo")
        .ZoneInfo("Asia/Ho_Chi_Minh")
    )
    .date()
    .isoformat()
)


ingestion_root = (
    f"{BRONZE_ROOT}/"
    f"ingestion_date={ingestion_date}/"
    f"ingestion_id={TEST_INGESTION_ID}"
)

bronze_prefix = (
    ingestion_root + "/"
)


cleanup_prefix(
    minio_client,
    bronze_prefix,
)


try:

    
    # 4. FIRST WRITE
    

    bronze_path = write_file_to_bronze(
        minio_client=minio_client,
        source_root=SOURCE_ROOT,
        source_file=source_file,
        ingestion_id=TEST_INGESTION_ID,
        ingested_at=TEST_INGESTED_AT,
    )


    print(
        "Bronze path:",
        bronze_path,
    )


    assert bronze_path == (
        f"{ingestion_root}/"
        "part-000.parquet"
    )


    
    # 5. READ-BACK BRONZE
    

    bronze_response = (
        minio_client.get_object(
            Bucket=BRONZE_BUCKET,
            Key=bronze_path,
        )
    )

    bronze_body = (
        bronze_response["Body"]
    )

    try:
        bronze_bytes = (
            bronze_body.read()
        )
    finally:
        bronze_body.close()


    if not bronze_bytes:
        raise AssertionError(
            "Bronze file is empty"
        )


    print(
        f"Bronze size: "
        f"{len(bronze_bytes):,} bytes"
    )


    bronze_df = pd.read_parquet(
        BytesIO(bronze_bytes),
        engine="pyarrow",
    )


    print(
        f"Bronze rows: "
        f"{len(bronze_df):,}"
    )

    print(
        "Bronze columns:",
        list(bronze_df.columns),
    )


    
    # 6. SOURCE PRESERVATION
    

    assert (
        len(bronze_df)
        == len(expected_df)
    )

    print(
        "Row count validation: PASS"
    )


    SOURCE_COLUMNS = [
        "session_id",
        "event_timestamp",
        "item_id",
        "category",
    ]


    assert bronze_df[
        SOURCE_COLUMNS
    ].equals(
        expected_df[
            SOURCE_COLUMNS
        ]
    )


    print(
        "Source payload validation: PASS"
    )


    
    # 7. METADATA CONTRACT
    

    EXPECTED_METADATA_COLUMNS = [
        "_source_name",
        "_source_file_path",
        "_source_file_etag",
        "_source_file_last_modified",
        "_source_file_size",
        "_ingestion_id",
        "_ingested_at",
        "_ingestion_method",
    ]


    for column in (
        EXPECTED_METADATA_COLUMNS
    ):
        if column not in bronze_df.columns:
            raise AssertionError(
                "Missing Bronze metadata "
                f"column: {column}"
            )


    print(
        "Metadata columns validation: PASS"
    )


    assert (
        bronze_df["_source_name"]
        == "yoochoose_clickstream"
    ).all()

    assert (
        bronze_df["_source_file_path"]
        == source_file.relative_path
    ).all()

    assert (
        bronze_df["_source_file_etag"]
        == source_file.etag
    ).all()

    assert (
        bronze_df["_source_file_size"]
        == source_file.size
    ).all()

    assert (
        bronze_df["_ingestion_id"]
        == TEST_INGESTION_ID
    ).all()

    assert (
        bronze_df["_ingestion_method"]
        == "file_incremental"
    ).all()


    print(
        "Metadata values validation: PASS"
    )


    
    # 8. RETRY SAME IDENTITY
    

    retry_bronze_path = (
        write_file_to_bronze(
            minio_client=minio_client,
            source_root=SOURCE_ROOT,
            source_file=source_file,
            ingestion_id=(
                TEST_INGESTION_ID
            ),
            ingested_at=(
                TEST_INGESTED_AT
            ),
        )
    )


    assert (
        retry_bronze_path
        == bronze_path
    )


    print(
        "Retry returned same "
        "Bronze path: PASS"
    )


    bronze_objects = (
        list_objects_under_prefix(
            minio_client,
            bronze_prefix,
        )
    )


    assert bronze_objects == {
        bronze_path,
    }


    print(
        "Retry duplicate validation: PASS"
    )


    print(
        "\n"
        "FILE BRONZE WRITER "
        "MINIO E2E: PASS"
    )


finally:

    cleanup_prefix(
        minio_client,
        bronze_prefix,
    )

    print(
        "File Bronze Writer "
        "test cleanup: PASS"
    )