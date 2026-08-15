from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from azure.storage.filedatalake import FileSystemClient

from fastorder.ingestion.file_based.file_discovery import (
    discover_files,
)

from fastorder.ingestion.file_based.manifest_manager import (
    load_manifest,
    save_manifest,
    find_manifest_entry,
    add_pending_entry,
    mark_processed,
)

from fastorder.ingestion.file_based.file_bronze_writer import (
    write_file_to_bronze,
)


@dataclass(frozen=True)
class FileIngestionResult:
    discovered: int
    processed: int
    skipped: int
    retried: int


def run_file_ingestion(
    *,
    landing_client: FileSystemClient,
    bronze_client: FileSystemClient,
    source_root: str,
    manifest_path: Path,
) -> FileIngestionResult:
    """
    Nạp gia tăng discovered Landing files vào Bronze bằng việc
    sử dụng manifest-based state management.
    """

    print(f"Đang nạp manifest từ {manifest_path}")
    manifest = load_manifest(manifest_path)
    print(f"Số lượng manifest nạp vào trong đợt này là {len(manifest.entries)}")


    print("Đang quét souce file tại Landing")
    discovered_files = discover_files(
        file_system_client=landing_client, 
        root_path=source_root
    )
    print(f"Tổng số file hợp lệ tìm trên Azure thấy là {len(discovered_files)}")


    processed = 0
    skipped = 0
    retried = 0

    for source_file in discovered_files:
        existing_entry = find_manifest_entry(
            manifest,
            source_file.relative_path,
            source_file.etag,
        )

        if existing_entry is None:
            print(
                f"Thêm mới manifest entry: {source_file.relative_path}"
            )
        else:
            print(
                f"Tìm thấy manifest entry: {source_file.relative_path} "
                f"status={existing_entry.status}"
            )

        if (existing_entry is not None and existing_entry.status == "PROCESSED"):
            skipped += 1
            print(
                f"SKIP: {source_file.relative_path} "
                f"đã được xử lý trước đó"
            )
            continue
        if (existing_entry is not None and existing_entry.status == "PENDING"):
            retried += 1
            ingestion_id = existing_entry.ingestion_id
            ingested_at = existing_entry.discovered_at
            print(
                f"RETRY: {source_file.relative_path} "
                f"ingestion_id={ingestion_id}"
            )
        if existing_entry is None:
            ingestion_id = str(uuid4())
            ingested_at = datetime.now(timezone.utc)

            manifest = add_pending_entry(
                manifest,
                relative_path=source_file.relative_path,
                etag=source_file.etag,
                size=source_file.size,
                last_modified=source_file.last_modified,
                ingestion_id=ingestion_id,
                discovered_at=ingested_at,
            )

            save_manifest(
                manifest,
                manifest_path,
            )

            print(
                f"PENDING: {source_file.relative_path} "
                f"ingestion_id={ingestion_id}"
            )

        bronze_path = write_file_to_bronze(
            landing_client=landing_client,
            bronze_client=bronze_client,
            source_root=source_root,
            source_file=source_file,
            ingestion_id=ingestion_id,
            ingested_at=ingested_at,
        )

        print(
            f"BRONZE: {source_file.relative_path} "
            f"-> {bronze_path}"
        )

        processed_at = datetime.now(timezone.utc)
        manifest = mark_processed(
            manifest,
            relative_path=source_file.relative_path,
            etag=source_file.etag,
            processed_at=processed_at,
        )

        save_manifest(
            manifest,
            manifest_path,
        )

        processed += 1

        print(f"PROCESSED: {source_file.relative_path}")

    result = FileIngestionResult(
        discovered=len(discovered_files),
        processed=processed,
        skipped=skipped,
        retried=retried,
    )

    print(
        "\nFile ingestion completed: "
        f"discovered={result.discovered}, "
        f"processed={result.processed}, "
        f"skipped={result.skipped}, "
        f"retried={result.retried}"
    )

    return result

