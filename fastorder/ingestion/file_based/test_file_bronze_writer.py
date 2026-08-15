from datetime import datetime, timezone
from io import BytesIO

import pandas as pd

from fastorder.storage.adls_client import get_adls_service_client
from fastorder.ingestion.file_based.file_discovery import discover_files
from fastorder.ingestion.file_based.file_bronze_writer import BRONZE_ROOT, write_file_to_bronze


SOURCE_ROOT = "clickstream/yoochoose/prepared"

TEST_INGESTION_ID = "test-file-bronze-writer-001"

TEST_INGESTED_AT = datetime(2026, 8, 13, 15, 0, 0, tzinfo=timezone.utc,)


service_client = get_adls_service_client()

landing_client = service_client.get_file_system_client("landing")

bronze_client = service_client.get_file_system_client("bronze")


files = discover_files(file_system_client=landing_client, root_path=SOURCE_ROOT)

if not files:
    raise RuntimeError(
        "không có YOOCHOOSE prepared CSV files discovered"
    )

source_file = files[0]

print("Đang kiểm tra source file:",source_file.relative_path)

source_path = (
    f"{SOURCE_ROOT}/"
    f"{source_file.relative_path}"
)

source_file_client = landing_client.get_file_client(source_path)

source_bytes = (
    source_file_client
    .download_file()
    .readall()
)

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

print(f"Source rows: {len(expected_df):,}")

bronze_path = write_file_to_bronze(
    landing_client=landing_client,
    bronze_client=bronze_client,
    source_root=SOURCE_ROOT,
    source_file=source_file,
    ingestion_id=TEST_INGESTION_ID,
    ingested_at=TEST_INGESTED_AT,
)

print("Bronze path:", bronze_path,)

bronze_file_client = bronze_client.get_file_client(bronze_path)

bronze_bytes = (
    bronze_file_client
    .download_file()
    .readall()
)

if not bronze_bytes:
    raise AssertionError(
        "Bronze file is empty"
    )

print(
    f"Bronze size: {len(bronze_bytes):,} bytes"
)


bronze_df = pd.read_parquet(
    BytesIO(bronze_bytes),
    engine="pyarrow",
)

print(
    f"Bronze rows: {len(bronze_df):,}"
)

print(
    "Bronze columns:",
    list(bronze_df.columns),
)


assert len(bronze_df) == len(expected_df)

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

for column in EXPECTED_METADATA_COLUMNS:
    if column not in bronze_df.columns:
        raise AssertionError(
            f"Missing Bronze metadata column: {column}"
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


print(
    "\nFile Bronze Writer E2E: PASS"
)


retry_bronze_path = write_file_to_bronze(
    landing_client=landing_client,
    bronze_client=bronze_client,
    source_root=SOURCE_ROOT,
    source_file=source_file,
    ingestion_id=TEST_INGESTION_ID,
    ingested_at=TEST_INGESTED_AT,
)

assert retry_bronze_path == bronze_path

print(
    "Retry returned same Bronze path: PASS"
)

ingestion_directory = (
    f"{BRONZE_ROOT}/"
    f"ingestion_date={TEST_INGESTED_AT.date().isoformat()}/"
    f"ingestion_id={TEST_INGESTION_ID}"
)

paths = list(
    bronze_client.get_paths(
        path=ingestion_directory,
        recursive=True,
    )
)

bronze_files = [
    path
    for path in paths
    if not path.is_directory
]

assert len(bronze_files) == 1, (
    "Retry created duplicate Bronze files: "
    f"found={len(bronze_files)}"
)

assert bronze_files[0].name == bronze_path

print(
    "Retry duplicate validation: PASS"
)

print(
    "\nFile Bronze Writer replay-safety: PASS"
)