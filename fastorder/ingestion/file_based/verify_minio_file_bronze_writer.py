from datetime import datetime
from io import BytesIO
from zoneinfo import ZoneInfo

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
    write_file_to_bronze,
)


SOURCE_ROOT = (
    "clickstream/yoochoose/prepared"
)

TEST_INGESTION_ID = (
    "test-minio-file-writer-001"
)


def main() -> None:

    client = get_minio_client()

    files = discover_files(
        minio_client=client,
        root_path=SOURCE_ROOT,
    )

    if not files:
        raise RuntimeError(
            "Không tìm thấy Landing files"
        )

    source_file = files[0]

    print(
        "Testing source:",
        source_file.relative_path,
    )

    ingested_at = datetime.now(
        tz=ZoneInfo("Asia/Ho_Chi_Minh")
    )

    bronze_path = write_file_to_bronze(
        minio_client=client,
        source_root=SOURCE_ROOT,
        source_file=source_file,
        ingestion_id=TEST_INGESTION_ID,
        ingested_at=ingested_at,
    )

    print("Bronze path:", bronze_path)

    response = client.get_object(
        Bucket=BRONZE_BUCKET,
        Key=bronze_path,
    )

    body = response["Body"]

    try:
        bronze_bytes = body.read()
    finally:
        body.close()

    if not bronze_bytes:
        raise RuntimeError(
            "Bronze object empty"
        )

    bronze_df = pd.read_parquet(
        BytesIO(bronze_bytes),
        engine="pyarrow",
    )

    print(
        "Bronze rows:",
        len(bronze_df),
    )

    print(
        "Bronze columns:",
        list(bronze_df.columns),
    )

    required_metadata = {
        "_source_name",
        "_source_file_path",
        "_source_file_etag",
        "_source_file_last_modified",
        "_source_file_size",
        "_ingestion_id",
        "_ingested_at",
        "_ingestion_method",
    }

    missing = (
        required_metadata
        - set(bronze_df.columns)
    )

    if missing:
        raise RuntimeError(
            f"Missing metadata: {missing}"
        )

    # Retry cùng ingestion_id.
    retry_path = write_file_to_bronze(
        minio_client=client,
        source_root=SOURCE_ROOT,
        source_file=source_file,
        ingestion_id=TEST_INGESTION_ID,
        ingested_at=ingested_at,
    )

    if retry_path != bronze_path:
        raise RuntimeError(
            "Retry returned different path"
        )

    prefix = (
        f"{BRONZE_ROOT}/"
        f"ingestion_date="
        f"{ingested_at.astimezone(
            ZoneInfo('Asia/Ho_Chi_Minh')
        ).date().isoformat()}/"
        f"ingestion_id={TEST_INGESTION_ID}/"
    )

    response = client.list_objects_v2(
        Bucket=BRONZE_BUCKET,
        Prefix=prefix,
    )

    objects = response.get(
        "Contents",
        [],
    )

    if len(objects) != 1:
        raise RuntimeError(
            "Retry tạo duplicate Bronze "
            f"objects: {len(objects)}"
        )

    print()
    print(
        "FILE BRONZE WRITER MINIO E2E: PASS"
    )

    # Cleanup test object.
    client.delete_object(
        Bucket=BRONZE_BUCKET,
        Key=bronze_path,
    )

    print(
        "TEST BRONZE OBJECT CLEANUP: PASS"
    )


if __name__ == "__main__":
    main()