from dataclasses import dataclass
from datetime import datetime
from azure.storage.filedatalake import FileSystemClient

@dataclass(frozen=True)
class DiscoveredFile:
    relative_path: str
    etag: str
    size: int
    last_modified: datetime

def discover_files(
    file_system_client: FileSystemClient,
    root_path: str,
) -> list[DiscoveredFile]:
    """
    Discover valid source CSV files recursively under root_path.
    """

    normalized_root = root_path.strip("/")
    root_prefix = normalized_root + "/"

    paths = file_system_client.get_paths(
        path=normalized_root,
        recursive=True,
    )

    discovered_files = []

    for path in paths:
        if path.is_directory:
            continue

        if not path.name.lower().endswith(".csv"):
            continue

        if not path.name.startswith(root_prefix):
            raise ValueError(
                f"Discovered path is outside root_path: {path.name}"
            )

        relative_path = path.name[len(root_prefix):]

        discovered_file = DiscoveredFile(
            relative_path=relative_path,
            etag=path.etag,
            size=path.content_length,
            last_modified=path.last_modified,
        )

        discovered_files.append(discovered_file)

    discovered_files.sort(
        key=lambda file: file.relative_path
    )

    return discovered_files