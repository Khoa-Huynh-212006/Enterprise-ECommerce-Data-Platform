from datetime import datetime, timezone
from uuid import uuid4

from fastorder.ingestion.api_based.weather_api_client import (
    fetch_forecast,
)

from fastorder.ingestion.api_based.weather_ingestion_metadata import (
    build_weather_ingestion_metadata,
)


requested_at = datetime.now(
    timezone.utc
)

result = fetch_forecast(
    latitude=10.82,
    longitude=106.63,
)


metadata = build_weather_ingestion_metadata(
    api_type="forecast",
    warehouse_id="WH_HCM",
    ingestion_id=str(uuid4()),
    requested_at=requested_at,
    api_result=result,
)


print(metadata)


assert metadata.source_name == "open_meteo"

assert metadata.api_type == "forecast"

assert metadata.warehouse_id == "WH_HCM"

assert metadata.requested_latitude == 10.82

assert metadata.requested_longitude == 106.63

assert metadata.response_latitude == (
    result.payload["latitude"]
)

assert metadata.response_longitude == (
    result.payload["longitude"]
)

assert metadata.http_status == 200


print(
    "\nWeather ingestion metadata test: PASS"
)