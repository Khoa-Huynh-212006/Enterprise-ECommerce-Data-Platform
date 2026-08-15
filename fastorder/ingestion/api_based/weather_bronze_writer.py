import json

from datetime import datetime
from zoneinfo import ZoneInfo

from azure.storage.filedatalake import FileSystemClient

from fastorder.ingestion.api_based.weather_api_client import (
    WeatherApiResult,
)

from fastorder.ingestion.api_based.weather_ingestion_metadata import (
    WeatherIngestionMetadata,
)


BRONZE_ROOT = "weather/open_meteo"
VN_TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")

def weather_metadata_to_dict(
    metadata: WeatherIngestionMetadata,
) -> dict:

    return {
        "version": metadata.version,
        "source_name": metadata.source_name,
        "api_type": metadata.api_type,
        "warehouse_id": metadata.warehouse_id,
        "ingestion_id": metadata.ingestion_id,
        "requested_at": metadata.requested_at.isoformat(),

        "requested_latitude": (
            metadata.requested_latitude
        ),
        "requested_longitude": (
            metadata.requested_longitude
        ),

        "response_latitude": (
            metadata.response_latitude
        ),
        "response_longitude": (
            metadata.response_longitude
        ),

        "endpoint": metadata.endpoint,
        "http_status": metadata.http_status,

        "request_params": metadata.request_params,
    }

def write_weather_to_bronze(
    *,
    bronze_client: FileSystemClient,
    api_result: WeatherApiResult,
    metadata: WeatherIngestionMetadata,
) -> tuple[str, str]:

    if not metadata.ingestion_id:
        raise ValueError("ingestion_id không được để trống")

    if (
        "/" in metadata.ingestion_id
        or "\\" in metadata.ingestion_id
    ):
        raise ValueError("ingestion_id không được chứa path separator")

    if not metadata.warehouse_id:
        raise ValueError("warehouse_id không được để trống")

    if (
        "/" in metadata.warehouse_id
        or "\\" in metadata.warehouse_id
    ):
        raise ValueError("warehouse_id không được chứa path separator")

    requested_at = metadata.requested_at

    if not isinstance(requested_at, datetime):
        raise ValueError("requested_at phải là datetime")

    if requested_at.tzinfo is None:
        raise ValueError("requested_at phải timezone-aware")

    ingestion_date = (
        requested_at
        .astimezone(VN_TIMEZONE)
        .date()
        .isoformat()
    )

    ingestion_root = (
        f"{BRONZE_ROOT}/"
        f"{metadata.api_type}/"
        f"ingestion_date={ingestion_date}/"
        f"warehouse_id={metadata.warehouse_id}/"
        f"ingestion_id={metadata.ingestion_id}"
    )

    response_path = (f"{ingestion_root}/response.json")
    metadata_path = (f"{ingestion_root}/metadata.json")

    response_bytes = json.dumps(
        api_result.payload,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")

    metadata_dict = weather_metadata_to_dict(metadata)

    metadata_bytes = json.dumps(
        metadata_dict,
        ensure_ascii=False,
        indent=2,
    ).encode("utf-8")

    response_file_client = bronze_client.get_file_client(response_path)
    metadata_file_client = bronze_client.get_file_client(metadata_path)

    response_file_client.upload_data(
        response_bytes,
        overwrite=True,
    )

    metadata_file_client.upload_data(
        metadata_bytes,
        overwrite=True,
    )

    return response_path, metadata_path