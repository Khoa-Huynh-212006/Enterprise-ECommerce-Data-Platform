from uuid import NAMESPACE_URL, uuid5

from azure.core.exceptions import ResourceNotFoundError

from azure.storage.filedatalake import FileSystemClient
from datetime import date

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
    bronze_client: FileSystemClient,
    ingestion_root: str,
) -> bool:

    success_path = (
        f"{ingestion_root}/_SUCCESS"
    )

    file_client = (
        bronze_client.get_file_client(
            success_path
        )
    )

    try:
        file_client.get_file_properties()
        return True

    except ResourceNotFoundError:
        return False


def write_success_marker(
    *,
    bronze_client: FileSystemClient,
    ingestion_root: str,
) -> str:

    success_path = (
        f"{ingestion_root}/_SUCCESS"
    )

    file_client = (
        bronze_client.get_file_client(
            success_path
        )
    )

    file_client.upload_data(
        b"COMMITTED\n",
        overwrite=True,
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

    if not isinstance(start_date, date):
        raise ValueError(
            "start_date phải là date"
        )

    if not isinstance(end_date, date):
        raise ValueError(
            "end_date phải là date"
        )

    if start_date > end_date:
        raise ValueError(
            "start_date không được lớn hơn end_date"
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