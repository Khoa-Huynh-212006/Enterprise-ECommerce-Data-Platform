from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from botocore.client import BaseClient

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
    minio_client: BaseClient,
    source_root: str,
    manifest_path: Path,
) -> FileIngestionResult:
    """
    Nạp gia tăng các Landing files vào Bronze
    bằng manifest-based state management.
    """

    print(
        f"Đang nạp manifest từ {manifest_path}"
    )

    manifest = load_manifest(
        manifest_path
    )

    print(
        "Số lượng manifest entries hiện tại: "
        f"{len(manifest.entries)}"
    )

    print(
        "Đang quét source files tại MinIO Landing"
    )

    discovered_files = discover_files(
        minio_client=minio_client,
        root_path=source_root,
    )

    print(
        "Tổng số source files được phát hiện: "
        f"{len(discovered_files)}"
    )

    processed = 0
    skipped = 0
    retried = 0

    for source_file in discovered_files:

        existing_entry = find_manifest_entry(
            manifest,
            source_file.relative_path,
            source_file.etag,
        )


        # 1. DETERMINE STATE


        if existing_entry is None:

            print(
                "Thêm mới manifest entry: "
                f"{source_file.relative_path}"
            )

            ingestion_id = str(
                uuid4()
            )

            ingested_at = datetime.now(
                timezone.utc
            )

            manifest = add_pending_entry(
                manifest,
                relative_path=(
                    source_file.relative_path
                ),
                etag=source_file.etag,
                size=source_file.size,
                last_modified=(
                    source_file.last_modified
                ),
                ingestion_id=ingestion_id,
                discovered_at=ingested_at,
            )

            # Quan trọng:
            # persist PENDING trước khi Bronze write.
            save_manifest(
                manifest,
                manifest_path,
            )

            print(
                f"PENDING: "
                f"{source_file.relative_path} "
                f"ingestion_id={ingestion_id}"
            )

        elif existing_entry.status == "PROCESSED":

            skipped += 1

            print(
                f"SKIP: "
                f"{source_file.relative_path} "
                "đã được xử lý trước đó"
            )

            continue

        elif existing_entry.status == "PENDING":

            retried += 1

            # Retry phải reuse đúng identity cũ.
            ingestion_id = (
                existing_entry.ingestion_id
            )

            ingested_at = (
                existing_entry.discovered_at
            )

            print(
                f"RETRY: "
                f"{source_file.relative_path} "
                f"ingestion_id={ingestion_id}"
            )

        else:
            raise ValueError(
                "Manifest status không hợp lệ: "
                f"path={source_file.relative_path}, "
                f"status={existing_entry.status}"
            )


        # 2. WRITE BRONZE


        bronze_path = write_file_to_bronze(
            minio_client=minio_client,
            source_root=source_root,
            source_file=source_file,
            ingestion_id=ingestion_id,
            ingested_at=ingested_at,
        )

        print(
            f"BRONZE: "
            f"{source_file.relative_path} "
            f"-> {bronze_path}"
        )


        # 3. MARK PROCESSED


        processed_at = datetime.now(
            timezone.utc
        )

        manifest = mark_processed(
            manifest,
            relative_path=(
                source_file.relative_path
            ),
            etag=source_file.etag,
            processed_at=processed_at,
        )

        save_manifest(
            manifest,
            manifest_path,
        )

        processed += 1

        print(
            f"PROCESSED: "
            f"{source_file.relative_path}"
        )

    result = FileIngestionResult(
        discovered=len(
            discovered_files
        ),
        processed=processed,
        skipped=skipped,
        retried=retried,
    )

    print()
    print(
        "File ingestion completed: "
        f"discovered={result.discovered}, "
        f"processed={result.processed}, "
        f"skipped={result.skipped}, "
        f"retried={result.retried}"
    )

    return result