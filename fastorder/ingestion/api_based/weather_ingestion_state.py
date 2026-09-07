from datetime import date
from uuid import NAMESPACE_URL, uuid5

from botocore.client import BaseClient
from botocore.exceptions import ClientError


BRONZE_BUCKET = "bronze"


def build_weather_ingestion_id(
    *,
    api_type: str,
    warehouse_id: str,
    run_id: str,
) -> str:

    if not api_type:
        raise ValueError(
            "api_type không được để trống"
        )

    if not warehouse_id:
        raise ValueError(
            "warehouse_id không được để trống"
        )

    if not run_id:
        raise ValueError(
            "run_id không được để trống"
        )

    identity_key = (
        f"fastorder:weather:"
        f"{api_type}:"
        f"{warehouse_id}:"
        f"{run_id}"
    )

    return str(
        uuid5(
            NAMESPACE_URL,
            identity_key,
        )
    )


def success_marker_exists(
    *,
    minio_client: BaseClient,
    ingestion_root: str,
) -> bool:

    success_path = (
        f"{ingestion_root}/_SUCCESS"
    )

    try:
        minio_client.head_object(
            Bucket=BRONZE_BUCKET,
            Key=success_path,
        )

        return True

    except ClientError as error:

        error_code = (
            error.response
            .get("Error", {})
            .get("Code")
        )

        if error_code in {
            "404",
            "NoSuchKey",
            "NotFound",
        }:
            return False

        raise


def write_success_marker(
    *,
    minio_client: BaseClient,
    ingestion_root: str,
) -> str:

    success_path = (
        f"{ingestion_root}/_SUCCESS"
    )

    minio_client.put_object(
        Bucket=BRONZE_BUCKET,
        Key=success_path,
        Body=b"COMMITTED\n",
        ContentType="text/plain",
    )

    return success_path


def build_historical_weather_ingestion_id(
    *,
    warehouse_id: str,
    start_date: date,
    end_date: date,
) -> str:

    if not warehouse_id:
        raise ValueError(
            "warehouse_id không được rỗng"
        )

    if not isinstance(
        start_date,
        date,
    ):
        raise ValueError(
            "start_date phải là date"
        )

    if not isinstance(
        end_date,
        date,
    ):
        raise ValueError(
            "end_date phải là date"
        )

    if start_date > end_date:
        raise ValueError(
            "start_date không được "
            "lớn hơn end_date"
        )

    identity_key = (
        "fastorder:weather:"
        "historical_forecast:"
        f"{warehouse_id}:"
        f"{start_date.isoformat()}:"
        f"{end_date.isoformat()}"
    )

    return str(
        uuid5(
            NAMESPACE_URL,
            identity_key,
        )
    )