from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from fastorder.storage.adls_client import (
    get_adls_service_client,
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
    BRONZE_ROOT,
    write_file_to_bronze,
)


SOURCE_ROOT = "clickstream/yoochoose/prepared"

TEST_MANIFEST_PATH = Path(
    "/opt/airflow/state/file_based/"
    "test_yoochoose_manifest.json"
)


if TEST_MANIFEST_PATH.exists():
    TEST_MANIFEST_PATH.unlink()


service_client = get_adls_service_client()

landing_client = service_client.get_file_system_client("landing")

bronze_client = service_client.get_file_system_client("bronze")

all_files = discover_files(file_system_client=landing_client, root_path=SOURCE_ROOT)

if len(all_files) < 4:
    raise RuntimeError(
        "Cần ít nhất 4 source files để test"
    )

test_files = all_files[:2]
pending_file = all_files[2]
post_bronze_crash_file = all_files[3]

print("Test files:")

for file in test_files:
    print(
        f"  - {file.relative_path}"
    )

with patch(
    "fastorder.ingestion.file_based."
    "file_ingestion_runner.discover_files",
    return_value=test_files,
):
    first_result = run_file_ingestion(
        landing_client=landing_client,
        bronze_client=bronze_client,
        source_root=SOURCE_ROOT,
        manifest_path=TEST_MANIFEST_PATH,
    )


print("\nFIRST RUN RESULT:")
print(first_result)


assert first_result.discovered == 2
assert first_result.processed == 2
assert first_result.skipped == 0
assert first_result.retried == 0

print(
    "First-run validation: PASS"
)


with patch(
    "fastorder.ingestion.file_based."
    "file_ingestion_runner.discover_files",
    return_value=test_files,
):
    second_result = run_file_ingestion(
        landing_client=landing_client,
        bronze_client=bronze_client,
        source_root=SOURCE_ROOT,
        manifest_path=TEST_MANIFEST_PATH,
    )


print("\nSECOND RUN RESULT:")
print(second_result)


assert second_result.discovered == 2
assert second_result.processed == 0
assert second_result.skipped == 2
assert second_result.retried == 0

print(
    "Second-run skip validation: PASS"
)

TEST_PENDING_INGESTION_ID = (
    "test-runner-pending-recovery-001"
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
    relative_path=pending_file.relative_path,
    etag=pending_file.etag,
    size=pending_file.size,
    last_modified=pending_file.last_modified,
    ingestion_id=TEST_PENDING_INGESTION_ID,
    discovered_at=TEST_PENDING_DISCOVERED_AT,
)

save_manifest(
    manifest,
    TEST_MANIFEST_PATH,
)

print(
    "\nSimulated PENDING entry:"
)

print(
    f"  file={pending_file.relative_path}"
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
        landing_client=landing_client,
        bronze_client=bronze_client,
        source_root=SOURCE_ROOT,
        manifest_path=TEST_MANIFEST_PATH,
    )


print("\nPENDING RECOVERY RESULT:")
print(retry_result)


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
    relative_path=pending_file.relative_path,
    etag=pending_file.etag,
)

assert recovered_entry is not None

assert recovered_entry.status == "PROCESSED"

assert (
    recovered_entry.ingestion_id
    == TEST_PENDING_INGESTION_ID
)

assert (
    recovered_entry.discovered_at
    == TEST_PENDING_DISCOVERED_AT
)

assert recovered_entry.processed_at is not None

print(
    "Pending recovery identity/state: PASS"
)

print(
    "\nRunner PENDING crash recovery: PASS"
)


POST_BRONZE_INGESTION_ID = (
    "test-runner-post-bronze-crash-001"
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
    relative_path=post_bronze_crash_file.relative_path,
    etag=post_bronze_crash_file.etag,
    size=post_bronze_crash_file.size,
    last_modified=post_bronze_crash_file.last_modified,
    ingestion_id=POST_BRONZE_INGESTION_ID,
    discovered_at=POST_BRONZE_DISCOVERED_AT,
)

save_manifest(
    manifest,
    TEST_MANIFEST_PATH,
)

preexisting_bronze_path = write_file_to_bronze(
    landing_client=landing_client,
    bronze_client=bronze_client,
    source_root=SOURCE_ROOT,
    source_file=post_bronze_crash_file,
    ingestion_id=POST_BRONZE_INGESTION_ID,
    ingested_at=POST_BRONZE_DISCOVERED_AT,
)

print(
    "\nSimulated Bronze success before crash:"
)

print(
    f"  bronze_path={preexisting_bronze_path}"
)

print(
    "  manifest_status=PENDING"
)

with patch(
    "fastorder.ingestion.file_based."
    "file_ingestion_runner.discover_files",
    return_value=[post_bronze_crash_file],
):
    post_bronze_retry_result = run_file_ingestion(
        landing_client=landing_client,
        bronze_client=bronze_client,
        source_root=SOURCE_ROOT,
        manifest_path=TEST_MANIFEST_PATH,
    )


print("\nPOST-BRONZE CRASH RECOVERY RESULT:")
print(post_bronze_retry_result)


assert post_bronze_retry_result.discovered == 1
assert post_bronze_retry_result.processed == 1
assert post_bronze_retry_result.skipped == 0
assert post_bronze_retry_result.retried == 1

print(
    "Post-Bronze retry counters: PASS"
)

ingestion_directory = (
    f"{BRONZE_ROOT}/"
    f"ingestion_date="
    f"{POST_BRONZE_DISCOVERED_AT.date().isoformat()}/"
    f"ingestion_id={POST_BRONZE_INGESTION_ID}"
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
    "Crash recovery tạo duplicate Bronze files: "
    f"found={len(bronze_files)}"
)

assert (
    bronze_files[0].name
    == preexisting_bronze_path
)

print(
    "Post-Bronze duplicate validation: PASS"
)

manifest_after_recovery = load_manifest(
    TEST_MANIFEST_PATH
)

recovered_entry = find_manifest_entry(
    manifest_after_recovery,
    relative_path=post_bronze_crash_file.relative_path,
    etag=post_bronze_crash_file.etag,
)

assert recovered_entry is not None
assert recovered_entry.status == "PROCESSED"

assert (
    recovered_entry.ingestion_id
    == POST_BRONZE_INGESTION_ID
)

assert (
    recovered_entry.discovered_at
    == POST_BRONZE_DISCOVERED_AT
)

assert recovered_entry.processed_at is not None

print(
    "Post-Bronze manifest recovery: PASS"
)

print(
    "\nRunner post-Bronze crash recovery: PASS"
)

# --------------------------------------------------
# 11. Cleanup test artifacts
# --------------------------------------------------

final_test_manifest = load_manifest(
    TEST_MANIFEST_PATH
)

for entry in final_test_manifest.entries:
    bronze_directory = (
        f"{BRONZE_ROOT}/"
        f"ingestion_date={entry.discovered_at.date().isoformat()}/"
        f"ingestion_id={entry.ingestion_id}"
    )

    directory_client = bronze_client.get_directory_client(
        bronze_directory
    )

    if directory_client.exists():
        directory_client.delete_directory()

        print(
            f"[CLEANUP] Deleted Bronze directory: "
            f"{bronze_directory}"
        )


if TEST_MANIFEST_PATH.exists():
    TEST_MANIFEST_PATH.unlink()

    print(
        f"[CLEANUP] Deleted test manifest: "
        f"{TEST_MANIFEST_PATH}"
    )


print(
    "\nRunner integration test cleanup: PASS"
)