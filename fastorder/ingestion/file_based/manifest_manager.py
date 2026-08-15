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


@dataclass(frozen=True) # giúp bạn không phải viết constructor thủ công
class ManifestEntry: # Một ManifestEntry đại diện cho một version cụ thể của một file
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

def manifest_entry_to_dict(entry: ManifestEntry) -> dict: # Chuyển các Manifest entry thành dict Python
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

def manifest_to_dict(manifest: FileManifest) -> dict: #Chuyển toàn bộ danh sách Manifest entry thành dict Python
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
            f"Không hỗ trợ manifest version này: {manifest.version}"
        )

    if manifest.source_name != YOOCHOOSE_SOURCE_NAME:
        raise ValueError(
            f"source_name không đúng: {manifest.source_name}"
        )

    seen_identities: set[tuple[str, str]] = set() # Kiểm tra duplicate

    for entry in manifest.entries:
        if not entry.relative_path.strip():
            raise ValueError(
                "Manifest entry có relative_path rỗng"
            )

        if not entry.etag.strip():
            raise ValueError(
                f"Manifest entry có etag rỗng: "
                f"{entry.relative_path}"
            )

        if entry.size < 0:
            raise ValueError(
                f"Manifest entry có kích thước âm: "
                f"{entry.relative_path}"
            )

        if entry.status not in ALLOWED_MANIFEST_STATUSES:
            raise ValueError(
                f"Manifest entry có status không thuộc PENDING hoặc PROCESSED"
                f"{entry.status!r}: {entry.relative_path}"
            )

        if not entry.ingestion_id.strip():
            raise ValueError(
                f"Manifest entry có ingestion_id rỗng: "
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
                f"PENDING entry không được có processed_at: "
                f"{entry.relative_path}"
            )

        if (
            entry.status == "PROCESSED"
            and entry.processed_at is None
        ):
            raise ValueError(
                f"PROCESSED entry phải có processed_at: "
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

def load_manifest(
    manifest_path: Path,
) -> FileManifest:
    """
    Tải và xác thực một manifest từ JSON. 
    Nếu manifest không tồn tại, trả về một manifest trống ban đầu.
    """

    if not manifest_path.exists():
        return create_initial_manifest()

    with manifest_path.open(
        "r",
        encoding="utf-8",
    ) as file:
        manifest_dict = json.load(file)

    manifest = manifest_from_dict(manifest_dict)

    validate_manifest(manifest)

    return manifest


def find_manifest_entry(
    manifest: FileManifest,
    relative_path: str,
    etag: str,
) -> ManifestEntry | None:
    """
    Tìm một manifest entry theo nhận dạng phiên bản tệp nguồn: (relative_path, etag).
    """

    for entry in manifest.entries:
        if (
            entry.relative_path == relative_path
            and entry.etag == etag
        ):
            return entry

    return None


def add_pending_entry(
    manifest: FileManifest,
    *,
    relative_path: str,
    etag: str,
    size: int,
    last_modified: datetime,
    ingestion_id: str,
    discovered_at: datetime,
) -> FileManifest:
    """
    Thêm một phiên bản tệp nguồn PENDING mới vào bản kê khai.
    Ném ValueError nếu cùng (relative_path, etag) đã tồn tại.
    """

    existing_entry = find_manifest_entry(
        manifest,
        relative_path=relative_path,
        etag=etag,
    )

    if existing_entry is not None:
        raise ValueError(
            "Manifest entry đã tồn tại "
            f"({relative_path}, {etag})"
        )

    pending_entry = ManifestEntry(
        relative_path=relative_path,
        etag=etag,
        size=size,
        last_modified=last_modified,
        status="PENDING",
        ingestion_id=ingestion_id,
        discovered_at=discovered_at,
        processed_at=None,
    )

    updated_manifest = FileManifest(
        version=manifest.version,
        source_name=manifest.source_name,
        entries=manifest.entries + (pending_entry,),
    )

    validate_manifest(updated_manifest)

    return updated_manifest


def mark_processed(
    manifest: FileManifest,
    *,
    relative_path: str,
    etag: str,
    processed_at: datetime,
) -> FileManifest:
    """
    Đánh dấu phiên bản tệp nguồn PENDING thành PROCESSED.
    """

    existing_entry = find_manifest_entry(
        manifest,
        relative_path=relative_path,
        etag=etag,
    )

    if existing_entry is None:
        raise ValueError(
            "Không thể đánh dấu PROCESSED cho 1 entry không tồn tại: "
            f"({relative_path}, {etag})"
        )

    if existing_entry.status != "PENDING":
        raise ValueError(
            "Chỉ có trạng thái PENDING mới được chuyển sang PROCESSED "
            f"{relative_path}"
        )

    processed_entry = ManifestEntry(
        relative_path=existing_entry.relative_path,
        etag=existing_entry.etag,
        size=existing_entry.size,
        last_modified=existing_entry.last_modified,

        status="PROCESSED",
        ingestion_id=existing_entry.ingestion_id,

        discovered_at=existing_entry.discovered_at,
        processed_at=processed_at,
    )

    updated_entries = tuple(
        processed_entry
        if (
            entry.relative_path == relative_path
            and entry.etag == etag
        )
        else entry
        for entry in manifest.entries
    )

    updated_manifest = FileManifest(
        version=manifest.version,
        source_name=manifest.source_name,
        entries=updated_entries,
    )

    validate_manifest(updated_manifest)

    return updated_manifest