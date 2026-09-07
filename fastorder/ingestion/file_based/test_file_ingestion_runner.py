from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4
from zoneinfo import ZoneInfo

from fastorder.storage.minio_client import (
    get_minio_client,
)

from fastorder.ingestion.file_based.file_discovery import (
    discover_files,
)

from fastorder.ingestion.file_based.file_ingestion_runner import (
    run_file_ingestion,
)

from fastorder.ingestion.file_based.manifest_manager import (
    load_manifest,
    save_manifest,
    add_pending_entry,
    find_manifest_entry,
)

from fastorder.ingestion.file_based.file_bronze_writer import (
    BRONZE_BUCKET,
    BRONZE_ROOT,
    write_file_to_bronze,
)


SOURCE_ROOT = (
    "clickstream/yoochoose/prepared"
)

TEST_MANIFEST_PATH = Path(
    "/opt/airflow/state/file_based/"
    "test_yoochoose_manifest.json"
)

TEST_RUN_ID = uuid4().hex[:8]


def get_ingestion_date(
    discovered_at: datetime,
) -> str:
    """
    Bronze Writer partition theo timezone
    Asia/Ho_Chi_Minh, nên test phải dùng
    cùng một rule.
    """

    return (
        discovered_at
        .astimezone(
            ZoneInfo("Asia/Ho_Chi_Minh")
        )
        .date()
        .isoformat()
    )


def build_bronze_prefix(
    *,
    discovered_at: datetime,
    ingestion_id: str,
) -> str:

    ingestion_date = get_ingestion_date(
        discovered_at
    )

    return (
        f"{BRONZE_ROOT}/"
        f"ingestion_date={ingestion_date}/"
        f"ingestion_id={ingestion_id}/"
    )



# 1. TEST SETUP


if TEST_MANIFEST_PATH.exists():
    TEST_MANIFEST_PATH.unlink()


minio_client = get_minio_client()


all_files = discover_files(
    minio_client=minio_client,
    root_path=SOURCE_ROOT,
)


if len(all_files) < 4:
    raise RuntimeError(
        "Cần ít nhất 4 source files để test"
    )


test_files = all_files[:2]

pending_file = all_files[2]

post_bronze_crash_file = (
    all_files[3]
)


print("Test files:")

for source_file in test_files:
    print(
        f"  - {source_file.relative_path}"
    )



# 2. FIRST RUN
#
# NEW
# → PENDING
# → Bronze
# → PROCESSED


with patch(
    "fastorder.ingestion.file_based."
    "file_ingestion_runner.discover_files",
    return_value=test_files,
):
    first_result = run_file_ingestion(
        minio_client=minio_client,
        source_root=SOURCE_ROOT,
        manifest_path=TEST_MANIFEST_PATH,
    )


print()
print("FIRST RUN RESULT:")
print(first_result)


assert first_result.discovered == 2
assert first_result.processed == 2
assert first_result.skipped == 0
assert first_result.retried == 0


print(
    "First-run validation: PASS"
)



# 3. SECOND RUN
#
# PROCESSED
# → SKIP


with patch(
    "fastorder.ingestion.file_based."
    "file_ingestion_runner.discover_files",
    return_value=test_files,
):
    second_result = run_file_ingestion(
        minio_client=minio_client,
        source_root=SOURCE_ROOT,
        manifest_path=TEST_MANIFEST_PATH,
    )


print()
print("SECOND RUN RESULT:")
print(second_result)


assert second_result.discovered == 2
assert second_result.processed == 0
assert second_result.skipped == 2
assert second_result.retried == 0


print(
    "Second-run skip validation: PASS"
)



# 4. PENDING CRASH RECOVERY


TEST_PENDING_INGESTION_ID = (
    f"test-runner-pending-recovery-"
    f"{TEST_RUN_ID}"
)


TEST_PENDING_DISCOVERED_AT = datetime(
    2026,
    8,
    15,
    16,
    0,
    0,
    tzinfo=timezone.utc,
)


manifest = load_manifest(
    TEST_MANIFEST_PATH
)


manifest = add_pending_entry(
    manifest,
    relative_path=(
        pending_file.relative_path
    ),
    etag=pending_file.etag,
    size=pending_file.size,
    last_modified=(
        pending_file.last_modified
    ),
    ingestion_id=(
        TEST_PENDING_INGESTION_ID
    ),
    discovered_at=(
        TEST_PENDING_DISCOVERED_AT
    ),
)


save_manifest(
    manifest,
    TEST_MANIFEST_PATH,
)


print()
print(
    "Simulated PENDING entry:"
)

print(
    f"  file="
    f"{pending_file.relative_path}"
)

print(
    f"  ingestion_id="
    f"{TEST_PENDING_INGESTION_ID}"
)


with patch(
    "fastorder.ingestion.file_based."
    "file_ingestion_runner.discover_files",
    return_value=[pending_file],
):
    retry_result = run_file_ingestion(
        minio_client=minio_client,
        source_root=SOURCE_ROOT,
        manifest_path=TEST_MANIFEST_PATH,
    )


print()
print(
    "PENDING RECOVERY RESULT:"
)

print(
    retry_result
)


assert retry_result.discovered == 1
assert retry_result.processed == 1
assert retry_result.skipped == 0
assert retry_result.retried == 1


print(
    "Pending retry counters: PASS"
)


manifest_after_retry = load_manifest(
    TEST_MANIFEST_PATH
)


recovered_entry = find_manifest_entry(
    manifest_after_retry,
    relative_path=(
        pending_file.relative_path
    ),
    etag=pending_file.etag,
)


assert recovered_entry is not None

assert (
    recovered_entry.status
    == "PROCESSED"
)

assert (
    recovered_entry.ingestion_id
    == TEST_PENDING_INGESTION_ID
)

assert (
    recovered_entry.discovered_at
    == TEST_PENDING_DISCOVERED_AT
)

assert (
    recovered_entry.processed_at
    is not None
)


print(
    "Pending recovery identity/state: PASS"
)

print(
    "Runner PENDING crash recovery: PASS"
)



# 5. POST-BRONZE CRASH RECOVERY
#
# Giả lập:
#
# Manifest = PENDING
# Bronze object = đã ghi thành công
# Process crash trước mark_processed()
#
# Retry phải:
# - reuse ingestion_id
# - reuse discovered_at
# - ghi cùng Bronze key
# - không tạo duplicate


POST_BRONZE_INGESTION_ID = (
    f"test-runner-post-bronze-crash-"
    f"{TEST_RUN_ID}"
)


POST_BRONZE_DISCOVERED_AT = datetime(
    2026,
    8,
    15,
    17,
    0,
    0,
    tzinfo=timezone.utc,
)


manifest = load_manifest(
    TEST_MANIFEST_PATH
)


manifest = add_pending_entry(
    manifest,
    relative_path=(
        post_bronze_crash_file.relative_path
    ),
    etag=(
        post_bronze_crash_file.etag
    ),
    size=(
        post_bronze_crash_file.size
    ),
    last_modified=(
        post_bronze_crash_file.last_modified
    ),
    ingestion_id=(
        POST_BRONZE_INGESTION_ID
    ),
    discovered_at=(
        POST_BRONZE_DISCOVERED_AT
    ),
)


save_manifest(
    manifest,
    TEST_MANIFEST_PATH,
)


preexisting_bronze_path = (
    write_file_to_bronze(
        minio_client=minio_client,
        source_root=SOURCE_ROOT,
        source_file=(
            post_bronze_crash_file
        ),
        ingestion_id=(
            POST_BRONZE_INGESTION_ID
        ),
        ingested_at=(
            POST_BRONZE_DISCOVERED_AT
        ),
    )
)


print()
print(
    "Simulated Bronze success "
    "before crash:"
)

print(
    f"  bronze_path="
    f"{preexisting_bronze_path}"
)

print(
    "  manifest_status=PENDING"
)


with patch(
    "fastorder.ingestion.file_based."
    "file_ingestion_runner.discover_files",
    return_value=[
        post_bronze_crash_file
    ],
):
    post_bronze_retry_result = (
        run_file_ingestion(
            minio_client=minio_client,
            source_root=SOURCE_ROOT,
            manifest_path=(
                TEST_MANIFEST_PATH
            ),
        )
    )


print()
print(
    "POST-BRONZE CRASH "
    "RECOVERY RESULT:"
)

print(
    post_bronze_retry_result
)


assert (
    post_bronze_retry_result.discovered
    == 1
)

assert (
    post_bronze_retry_result.processed
    == 1
)

assert (
    post_bronze_retry_result.skipped
    == 0
)

assert (
    post_bronze_retry_result.retried
    == 1
)


print(
    "Post-Bronze retry counters: PASS"
)



# 6. VERIFY NO DUPLICATE BRONZE OBJECT


bronze_prefix = build_bronze_prefix(
    discovered_at=(
        POST_BRONZE_DISCOVERED_AT
    ),
    ingestion_id=(
        POST_BRONZE_INGESTION_ID
    ),
)


response = minio_client.list_objects_v2(
    Bucket=BRONZE_BUCKET,
    Prefix=bronze_prefix,
)


bronze_objects = response.get(
    "Contents",
    [],
)


assert len(bronze_objects) == 1, (
    "Crash recovery tạo duplicate "
    "Bronze objects: "
    f"found={len(bronze_objects)}"
)


assert (
    bronze_objects[0]["Key"]
    == preexisting_bronze_path
)


print(
    "Post-Bronze duplicate "
    "validation: PASS"
)



# 7. VERIFY MANIFEST AFTER RECOVERY


manifest_after_recovery = (
    load_manifest(
        TEST_MANIFEST_PATH
    )
)


recovered_entry = find_manifest_entry(
    manifest_after_recovery,
    relative_path=(
        post_bronze_crash_file.relative_path
    ),
    etag=(
        post_bronze_crash_file.etag
    ),
)


assert recovered_entry is not None

assert (
    recovered_entry.status
    == "PROCESSED"
)

assert (
    recovered_entry.ingestion_id
    == POST_BRONZE_INGESTION_ID
)

assert (
    recovered_entry.discovered_at
    == POST_BRONZE_DISCOVERED_AT
)

assert (
    recovered_entry.processed_at
    is not None
)


print(
    "Post-Bronze manifest recovery: PASS"
)

print(
    "Runner post-Bronze "
    "crash recovery: PASS"
)



# 8. CLEANUP TEST ARTIFACTS


final_test_manifest = load_manifest(
    TEST_MANIFEST_PATH
)


for entry in final_test_manifest.entries:

    bronze_prefix = build_bronze_prefix(
        discovered_at=(
            entry.discovered_at
        ),
        ingestion_id=(
            entry.ingestion_id
        ),
    )

    response = (
        minio_client.list_objects_v2(
            Bucket=BRONZE_BUCKET,
            Prefix=bronze_prefix,
        )
    )

    objects = response.get(
        "Contents",
        [],
    )

    for object_info in objects:

        object_key = (
            object_info["Key"]
        )

        minio_client.delete_object(
            Bucket=BRONZE_BUCKET,
            Key=object_key,
        )

        print(
            "[CLEANUP] Deleted "
            "Bronze object: "
            f"{object_key}"
        )


if TEST_MANIFEST_PATH.exists():

    TEST_MANIFEST_PATH.unlink()

    print(
        "[CLEANUP] Deleted "
        "test manifest: "
        f"{TEST_MANIFEST_PATH}"
    )


print()
print(
    "Runner integration "
    "test cleanup: PASS"
)

print()
print(
    "FILE INGESTION RUNNER "
    "MINIO INTEGRATION: PASS"
)