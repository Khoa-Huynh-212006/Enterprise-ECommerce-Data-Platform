from uuid import NAMESPACE_URL, uuid5

from azure.core.exceptions import ResourceNotFoundError

from azure.storage.filedatalake import FileSystemClient


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