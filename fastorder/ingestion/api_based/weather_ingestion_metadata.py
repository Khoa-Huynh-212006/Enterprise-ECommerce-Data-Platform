from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from fastorder.ingestion.api_based.weather_api_client import WeatherApiResult


WeatherApiType = Literal[ 
    "forecast",
    "historical_forecast"
]
# WeatherApiType chỉ được phép có một trong hai giá trị: "forecast" hoặc "historical_forecast"


METADATA_VERSION = 1
SOURCE_NAME = "open_meteo"


@dataclass(frozen=True)
class WeatherIngestionMetadata:
    version: int

    source_name: str
    api_type: WeatherApiType

    warehouse_id: str
    ingestion_id: str

    requested_at: datetime

    requested_latitude: float
    requested_longitude: float

    response_latitude: float
    response_longitude: float

    endpoint: str
    http_status: int

    request_params: dict[str, Any]

def build_weather_ingestion_metadata(
    *,
    api_type: WeatherApiType,
    warehouse_id: str,
    ingestion_id: str,
    requested_at: datetime,
    api_result: WeatherApiResult,
) -> WeatherIngestionMetadata:

    if not warehouse_id:
        raise ValueError(
            "warehouse_id không được để trống"
        )

    if not ingestion_id:
        raise ValueError(
            "ingestion_id không được để trống"
        )

    if not isinstance(requested_at, datetime):
        raise ValueError("requested_at phải là datetime")

    request_params = api_result.request_params
    payload = api_result.payload

    requested_latitude = request_params.get("latitude")

    requested_longitude = request_params.get("longitude")

    response_latitude = payload.get("latitude")

    response_longitude = payload.get("longitude")

    if requested_latitude is None:
        raise ValueError("Request params thiếu latitude")

    if requested_longitude is None:
        raise ValueError("Request params thiếu longitude")

    if response_latitude is None:
        raise ValueError("Open-Meteo response thiếu latitude")

    if response_longitude is None:
        raise ValueError("Open-Meteo response thiếu longitude")

    return WeatherIngestionMetadata(
        version=METADATA_VERSION,
        source_name=SOURCE_NAME,
        api_type=api_type,

        warehouse_id=warehouse_id,
        ingestion_id=ingestion_id,

        requested_at=requested_at,

        requested_latitude=float(requested_latitude),
        requested_longitude=float(requested_longitude),

        response_latitude=float(response_latitude),
        response_longitude=float(response_longitude),

        endpoint=api_result.endpoint,
        http_status=api_result.status_code,

        request_params=dict(api_result.request_params)
    )