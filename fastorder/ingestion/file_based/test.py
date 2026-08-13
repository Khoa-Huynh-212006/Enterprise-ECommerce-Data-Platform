import json
from datetime import datetime, timezone
from fastorder.ingestion.file_based.manifest_manager import (
    FileManifest,
    ManifestEntry,
    manifest_to_dict,
    manifest_from_dict,
    validate_manifest,
    save_manifest,
    load_manifest,
    find_manifest_entry
)
from pathlib import Path


sample_entry = ManifestEntry(
    relative_path=(
        "event_date=2014-08-10/"
        "part-00000.csv"
    ),
    etag='"0x8D123456"',
    size=15_000_000,
    last_modified=datetime.now(timezone.utc),

    status="PENDING",
    ingestion_id="test-ingestion-001",

    discovered_at=datetime.now(timezone.utc),
    processed_at=None,
)


original_manifest = FileManifest(
    version=1,
    source_name="yoochoose_clickstream",
    entries=(sample_entry,),
)

manifest_dict = manifest_to_dict(
    original_manifest
)

json_text = json.dumps(
    manifest_dict,
    indent=2,
)

print("Serialized JSON:")
print(json_text)

loaded_dict = json.loads(json_text)

restored_manifest = manifest_from_dict(
    loaded_dict
)

print("\nRestored manifest:")
print(restored_manifest)

assert restored_manifest == original_manifest
print("\nManifest round-trip: PASS")

validate_manifest(restored_manifest)

print("Manifest validation: PASS")


TEST_MANIFEST_PATH = Path(
    "/opt/airflow/state/file_based/"
    "test_yoochoose_manifest.json"
)
MISSING_MANIFEST_PATH = Path(
    "/opt/airflow/state/file_based/"
    "missing_manifest_test.json"
)
if MISSING_MANIFEST_PATH.exists():
    MISSING_MANIFEST_PATH.unlink()

initial_manifest = load_manifest(
    MISSING_MANIFEST_PATH
)

assert initial_manifest.version == 1
assert initial_manifest.source_name == "yoochoose_clickstream"
assert len(initial_manifest.entries) == 0

print("Missing manifest -> initial state: PASS")

CORRUPT_MANIFEST_PATH = Path(
    "/opt/airflow/state/file_based/"
    "corrupt_manifest_test.json"
)

CORRUPT_MANIFEST_PATH.parent.mkdir(
    parents=True,
    exist_ok=True,
)

with CORRUPT_MANIFEST_PATH.open(
    "w",
    encoding="utf-8",
) as file:
    file.write(
        '{"version": 1, "source_name": '
    )
    try:
        load_manifest(
            CORRUPT_MANIFEST_PATH
        )

    except json.JSONDecodeError:
        print(
            "Corrupt manifest correctly rejected: PASS"
        )

    else:
        raise AssertionError(
            "Corrupt manifest was not rejected"
        )

if CORRUPT_MANIFEST_PATH.exists():
    CORRUPT_MANIFEST_PATH.unlink()

save_manifest(
    original_manifest,
    TEST_MANIFEST_PATH,
)

print(
    f"Manifest saved to: "
    f"{TEST_MANIFEST_PATH}"
)

assert TEST_MANIFEST_PATH.exists()
print("Manifest file exists: PASS")

temp_path = TEST_MANIFEST_PATH.with_suffix(
    TEST_MANIFEST_PATH.suffix + ".tmp"
)

assert not temp_path.exists()
print("Temporary file cleanup: PASS")

loaded_manifest = load_manifest(
    TEST_MANIFEST_PATH
)

assert loaded_manifest == original_manifest
print("Manifest load: PASS")

found_entry = find_manifest_entry(
    original_manifest,
    relative_path=sample_entry.relative_path,
    etag=sample_entry.etag,
)

assert found_entry == sample_entry
print("Manifest lookup existing entry: PASS")


different_version = find_manifest_entry(
    original_manifest,
    relative_path=sample_entry.relative_path,
    etag='"DIFFERENT-ETAG"',
)

assert different_version is None
print("Manifest lookup different version: PASS")


missing_entry = find_manifest_entry(
    original_manifest,
    relative_path="event_date=2099-01-01/new-file.csv",
    etag='"NEW-ETAG"',
)

assert missing_entry is None
print("Manifest lookup missing entry: PASS")