from dataclasses import dataclass
from datetime import datetime

from botocore.client import BaseClient


LANDING_BUCKET = "landing"


@dataclass(frozen=True)
class DiscoveredFile:
    relative_path: str
    etag: str
    size: int
    last_modified: datetime


def discover_files(
    minio_client: BaseClient,
    root_path: str,
) -> list[DiscoveredFile]:
    """
    Tìm tất cả file CSV hợp lệ dưới root_path
    trong MinIO Landing bucket.
    """

    normalized_root = root_path.strip("/")

    if not normalized_root:
        raise ValueError(
            "root_path không được rỗng."
        )

    root_prefix = normalized_root + "/"

    discovered_files: list[DiscoveredFile] = []

    continuation_token = None

    while True:
        request = {
            "Bucket": LANDING_BUCKET,
            "Prefix": root_prefix,
        }

        if continuation_token:
            request["ContinuationToken"] = (
                continuation_token
            )

        response = minio_client.list_objects_v2(
            **request
        )

        for item in response.get(
            "Contents",
            [],
        ):
            object_key = item["Key"]

            if not object_key.lower().endswith(
                ".csv"
            ):
                continue

            if not object_key.startswith(
                root_prefix
            ):
                raise ValueError(
                    "Đường dẫn được phát hiện "
                    "nằm ngoài root_path: "
                    f"{object_key}"
                )

            relative_path = object_key[
                len(root_prefix):
            ]

            discovered_file = DiscoveredFile(
                relative_path=relative_path,
                etag=item["ETag"],
                size=item["Size"],
                last_modified=item[
                    "LastModified"
                ],
            )

            discovered_files.append(
                discovered_file
            )

        if not response.get(
            "IsTruncated",
            False,
        ):
            break

        continuation_token = response[
            "NextContinuationToken"
        ]

    discovered_files.sort(
        key=lambda file: file.relative_path
    )

    return discovered_files