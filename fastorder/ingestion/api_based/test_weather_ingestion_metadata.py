from datetime import datetime, timezone

from fastorder.ingestion.api_based.weather_api_client import (
    WeatherApiResult,
)

from fastorder.ingestion.api_based.weather_ingestion_metadata import (
    build_weather_ingestion_metadata,
)


TEST_TIME = datetime(
    2026,
    8,
    15,
    20,
    0,
    tzinfo=timezone.utc,
)


fake_result = WeatherApiResult(
    payload={
        "latitude": 10.790861,
        "longitude": 106.6313,
    },
    status_code=200,
    endpoint=(
        "https://api.open-meteo.com/v1/forecast"
    ),
    request_params={
        "latitude": 10.82,
        "longitude": 106.63,
    },
)


metadata = build_weather_ingestion_metadata(
    api_type="forecast",
    warehouse_id="WH_HCM",
    run_id="test_weather_run_001",
    ingestion_id="test_weather_metadata_001",

    logical_at=TEST_TIME,
    requested_at=TEST_TIME,

    api_result=fake_result,
)


print(metadata)


assert metadata.version == 1
assert metadata.source_name == "open_meteo"
assert metadata.api_type == "forecast"
assert metadata.warehouse_id == "WH_HCM"

assert metadata.run_id == "test_weather_run_001"
assert metadata.ingestion_id == "test_weather_metadata_001"

assert metadata.logical_at == TEST_TIME
assert metadata.requested_at == TEST_TIME

assert metadata.requested_latitude == 10.82
assert metadata.requested_longitude == 106.63

assert (
    metadata.response_latitude
    == fake_result.payload["latitude"]
)

assert (
    metadata.response_longitude
    == fake_result.payload["longitude"]
)

assert metadata.http_status == 200


print(
    "\nWeather ingestion metadata test: PASS"
)