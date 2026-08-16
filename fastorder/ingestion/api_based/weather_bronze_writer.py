import json

from datetime import datetime, date
from zoneinfo import ZoneInfo

from azure.storage.filedatalake import (
    FileSystemClient,
)

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
        "run_id": metadata.run_id,
        "ingestion_id": metadata.ingestion_id,

        "logical_at": metadata.logical_at.isoformat(),
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


def build_weather_ingestion_root(
    *,
    api_type: str,
    warehouse_id: str,
    ingestion_id: str,
    logical_at: datetime,
) -> str:

    if not isinstance(logical_at, datetime):
        raise ValueError(
            "logical_at phải là datetime"
        )

    if logical_at.tzinfo is None:
        raise ValueError(
            "logical_at phải timezone-aware"
        )

    ingestion_date = (
        logical_at
        .astimezone(VN_TIMEZONE)
        .date()
        .isoformat()
    )

    return (
        f"{BRONZE_ROOT}/"
        f"{api_type}/"
        f"ingestion_date={ingestion_date}/"
        f"warehouse_id={warehouse_id}/"
        f"ingestion_id={ingestion_id}"
    )


def build_historical_weather_ingestion_root(
    *,
    warehouse_id: str,
    ingestion_id: str,
    start_date: date,
    end_date: date,
) -> str:

    if not warehouse_id:
        raise ValueError(
            "warehouse_id không được rỗng"
        )

    if not ingestion_id:
        raise ValueError(
            "ingestion_id không được rỗng"
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

    return (
        f"{BRONZE_ROOT}/historical_forecast/"
        f"window_start={start_date.isoformat()}/"
        f"window_end={end_date.isoformat()}/"
        f"warehouse_id={warehouse_id}/"
        f"ingestion_id={ingestion_id}"
    )


def _write_weather_files(
    *,
    bronze_client: FileSystemClient,
    api_result: WeatherApiResult,
    metadata: WeatherIngestionMetadata,
    ingestion_root: str,
) -> tuple[str, str]:

    if not metadata.ingestion_id:
        raise ValueError(
            "ingestion_id không được để trống"
        )

    if (
        "/" in metadata.ingestion_id
        or "\\" in metadata.ingestion_id
    ):
        raise ValueError(
            "ingestion_id không được chứa path separator"
        )

    if not metadata.warehouse_id:
        raise ValueError(
            "warehouse_id không được để trống"
        )

    if (
        "/" in metadata.warehouse_id
        or "\\" in metadata.warehouse_id
    ):
        raise ValueError(
            "warehouse_id không được chứa path separator"
        )

    response_path = (
        f"{ingestion_root}/response.json"
    )

    metadata_path = (
        f"{ingestion_root}/metadata.json"
    )

    response_bytes = json.dumps(
        api_result.payload,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")

    metadata_bytes = json.dumps(
        weather_metadata_to_dict(metadata),
        ensure_ascii=False,
        indent=2,
    ).encode("utf-8")

    bronze_client.get_file_client(
        response_path
    ).upload_data(
        response_bytes,
        overwrite=True,
    )

    bronze_client.get_file_client(
        metadata_path
    ).upload_data(
        metadata_bytes,
        overwrite=True,
    )

    return (
        response_path,
        metadata_path,
    )


def write_weather_to_bronze(
    *,
    bronze_client: FileSystemClient,
    api_result: WeatherApiResult,
    metadata: WeatherIngestionMetadata,
) -> tuple[str, str]:

    ingestion_root = build_weather_ingestion_root(
        api_type=metadata.api_type,
        warehouse_id=metadata.warehouse_id,
        ingestion_id=metadata.ingestion_id,
        logical_at=metadata.logical_at,
    )

    return _write_weather_files(
        bronze_client=bronze_client,
        api_result=api_result,
        metadata=metadata,
        ingestion_root=ingestion_root,
    )


def write_historical_weather_to_bronze(
    *,
    bronze_client: FileSystemClient,
    api_result: WeatherApiResult,
    metadata: WeatherIngestionMetadata,
    start_date: date,
    end_date: date,
) -> tuple[str, str]:

    ingestion_root = (
        build_historical_weather_ingestion_root(
            warehouse_id=metadata.warehouse_id,
            ingestion_id=metadata.ingestion_id,
            start_date=start_date,
            end_date=end_date,
        )
    )

    return _write_weather_files(
        bronze_client=bronze_client,
        api_result=api_result,
        metadata=metadata,
        ingestion_root=ingestion_root,
    )