from dataclasses import dataclass
from datetime import datetime
from typing import Literal


ManifestStatus = Literal[
    "PENDING",
    "PROCESSED",
]


MANIFEST_VERSION = 1
YOOCHOOSE_SOURCE_NAME = "yoochoose_clickstream"


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