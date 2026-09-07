from datetime import date
from pathlib import Path

from botocore.client import BaseClient
from botocore.exceptions import ClientError

from fastorder.storage.minio_client import (
    get_minio_client,
)


LANDING_BUCKET = "landing"

LANDING_PREFIX = (
    "clickstream/yoochoose/prepared"
)

TEMP_ROOT = Path(
    "/opt/airflow/data/tmp/yoochoose_prepare"
)

EXPECTED_FILES = 183


def get_remote_object_size(
    minio_client: BaseClient,
    object_key: str,
) -> int | None:
    try:
        response = minio_client.head_object(
            Bucket=LANDING_BUCKET,
            Key=object_key,
        )

        return response["ContentLength"]

    except ClientError as exc:
        error_code = str(
            exc.response["Error"]["Code"]
        )

        if error_code in {
            "404",
            "NoSuchKey",
            "NotFound",
        }:
            return None

        raise


def upload_yoochoose_landing(
    minio_client: BaseClient,
    temp_root: Path,
) -> None:

    if not temp_root.exists():
        raise FileNotFoundError(
            f"Temp root không tồn tại: {temp_root}"
        )

    csv_files = sorted(
        temp_root.glob("*.csv")
    )

    if len(csv_files) != EXPECTED_FILES:
        raise RuntimeError(
            "Unexpected temp CSV count: "
            f"{len(csv_files)}"
        )

    uploaded = 0
    skipped = 0
    total_bytes = 0

    for index, local_file in enumerate(
        csv_files,
        start=1,
    ):
        event_date = local_file.stem

        # Fail-fast nếu tên file không phải YYYY-MM-DD.
        date.fromisoformat(event_date)

        object_key = (
            f"{LANDING_PREFIX}/"
            f"event_date={event_date}/"
            "part-000.csv"
        )

        local_size = local_file.stat().st_size

        remote_size = get_remote_object_size(
            minio_client=minio_client,
            object_key=object_key,
        )

        if remote_size is not None:

            if remote_size != local_size:
                raise RuntimeError(
                    "Landing object đã tồn tại "
                    "nhưng size không khớp: "
                    f"{object_key}, "
                    f"local={local_size}, "
                    f"remote={remote_size}"
                )

            skipped += 1
            total_bytes += local_size

            print(
                f"[{index:03d}/{len(csv_files)}] "
                f"SKIP {object_key}"
            )

            continue

        minio_client.upload_file(
            Filename=str(local_file),
            Bucket=LANDING_BUCKET,
            Key=object_key,
        )

        verified_size = get_remote_object_size(
            minio_client=minio_client,
            object_key=object_key,
        )

        if verified_size != local_size:
            raise RuntimeError(
                "Upload verification failed: "
                f"{object_key}, "
                f"local={local_size}, "
                f"remote={verified_size}"
            )

        uploaded += 1
        total_bytes += local_size

        print(
            f"[{index:03d}/{len(csv_files)}] "
            f"UPLOAD {object_key}"
        )

    print()
    print("YOOCHOOSE MINIO LANDING UPLOAD")
    print(f"Files discovered : {len(csv_files):,}")
    print(f"Uploaded         : {uploaded:,}")
    print(f"Skipped          : {skipped:,}")
    print(f"Total bytes      : {total_bytes:,}")

    print()
    print(
        "YOOCHOOSE MINIO LANDING UPLOAD: PASS"
    )


if __name__ == "__main__":
    client = get_minio_client()

    upload_yoochoose_landing(
        minio_client=client,
        temp_root=TEMP_ROOT,
    )