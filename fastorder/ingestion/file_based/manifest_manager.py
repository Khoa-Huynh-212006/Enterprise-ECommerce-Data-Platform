from dataclasses import dataclass
from datetime import datetime
from typing import Literal, cast
import json
import os
from pathlib import Path

ManifestStatus = Literal[
    "PENDING",
    "PROCESSED",
]


MANIFEST_VERSION = 1
YOOCHOOSE_SOURCE_NAME = "yoochoose_clickstream"

ALLOWED_MANIFEST_STATUSES = {
    "PENDING",
    "PROCESSED",
}


@dataclass(frozen=True)
class ManifestEntry:
    relative_path: str
    etag: str
    size: int
    last_modified: datetime

    status: ManifestStatus
    ingestion_id: str

    discovered_at: datetime
    processed_at: datetime | None


@dataclass(frozen=True)
class FileManifest:
    version: int
    source_name: str
    entries: tuple[ManifestEntry, ...]


def create_initial_manifest() -> FileManifest:
    return FileManifest(
        version=MANIFEST_VERSION,
        source_name=YOOCHOOSE_SOURCE_NAME,
        entries=(),
    )

def manifest_entry_to_dict(entry: ManifestEntry) -> dict:
    return {
        "relative_path": entry.relative_path,
        "etag": entry.etag,
        "size": entry.size,
        "last_modified": entry.last_modified.isoformat(),
        "status": entry.status,
        "ingestion_id": entry.ingestion_id,
        "discovered_at": entry.discovered_at.isoformat(),
        "processed_at": (
            entry.processed_at.isoformat()
            if entry.processed_at is not None
            else None
        ),
    }

def manifest_to_dict(manifest: FileManifest) -> dict:
    return {
        "version": manifest.version,
        "source_name": manifest.source_name,
        "entries": [
            manifest_entry_to_dict(entry)
            for entry in manifest.entries
        ],
    }

def manifest_entry_from_dict(data: dict) -> ManifestEntry:
    return ManifestEntry(
        relative_path=data["relative_path"],
        etag=data["etag"],
        size=data["size"],
        last_modified=datetime.fromisoformat(
            data["last_modified"]
        ),
        status=cast(
            ManifestStatus,
            data["status"]
        ),
        ingestion_id=data["ingestion_id"],
        discovered_at=datetime.fromisoformat(
            data["discovered_at"]
        ),
        processed_at=(
            datetime.fromisoformat(data["processed_at"])
            if data["processed_at"] is not None
            else None
        ),
    )

def manifest_from_dict(data: dict) -> FileManifest:
    return FileManifest(
        version=data["version"],
        source_name=data["source_name"],
        entries=tuple(
            manifest_entry_from_dict(entry)
            for entry in data["entries"]
        ),
    )

def validate_manifest(manifest: FileManifest) -> None:
    if manifest.version != MANIFEST_VERSION:
        raise ValueError(
            f"Unsupported manifest version: {manifest.version}"
        )

    if manifest.source_name != YOOCHOOSE_SOURCE_NAME:
        raise ValueError(
            f"Unexpected source_name: {manifest.source_name}"
        )

    seen_identities: set[tuple[str, str]] = set()

    for entry in manifest.entries:

        if not entry.relative_path.strip():
            raise ValueError(
                "Manifest entry has empty relative_path"
            )

        if not entry.etag.strip():
            raise ValueError(
                f"Manifest entry has empty etag: "
                f"{entry.relative_path}"
            )

        if entry.size < 0:
            raise ValueError(
                f"Manifest entry has negative size: "
                f"{entry.relative_path}"
            )

        if entry.status not in ALLOWED_MANIFEST_STATUSES:
            raise ValueError(
                f"Invalid manifest status "
                f"{entry.status!r}: {entry.relative_path}"
            )

        if not entry.ingestion_id.strip():
            raise ValueError(
                f"Manifest entry has empty ingestion_id: "
                f"{entry.relative_path}"
            )

        identity = (
            entry.relative_path,
            entry.etag,
        )

        if identity in seen_identities:
            raise ValueError(
                f"Duplicate manifest identity: {identity}"
            )

        seen_identities.add(identity)

        if (
            entry.status == "PENDING"
            and entry.processed_at is not None
        ):
            raise ValueError(
                f"PENDING entry must not have processed_at: "
                f"{entry.relative_path}"
            )

        if (
            entry.status == "PROCESSED"
            and entry.processed_at is None
        ):
            raise ValueError(
                f"PROCESSED entry must have processed_at: "
                f"{entry.relative_path}"
            )

def save_manifest(
    manifest: FileManifest,
    manifest_path: Path,
) -> None:
    """
    Validate and atomically persist a manifest as JSON.
    """

    validate_manifest(manifest)

    manifest_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_path = manifest_path.with_suffix(
        manifest_path.suffix + ".tmp"
    )

    manifest_dict = manifest_to_dict(manifest)

    with temp_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            manifest_dict,
            file,
            indent=2,
            ensure_ascii=False,
        )

        file.flush()
        os.fsync(file.fileno())

    os.replace(
        temp_path,
        manifest_path,
    )