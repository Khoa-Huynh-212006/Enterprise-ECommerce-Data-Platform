import json

from datetime import datetime, timezone

from fastorder.storage.adls_client import (
    get_adls_service_client,
)

from fastorder.ingestion.api_based.weather_api_client import (
    WeatherApiResult,
)

from fastorder.ingestion.api_based.weather_ingestion_metadata import (
    build_weather_ingestion_metadata,
)

from fastorder.ingestion.api_based.weather_bronze_writer import (
    write_weather_to_bronze,
)


TEST_INGESTION_ID = (
    "test_weather_bronze_writer_001"
)

TEST_REQUESTED_AT = datetime(
    2026,
    8,
    15,
    20,
    0,
    tzinfo=timezone.utc,
)


fake_payload = {
    "latitude": 10.790861,
    "longitude": 106.6313,
    "generationtime_ms": 0.1,
    "utc_offset_seconds": 25200,
    "timezone": "Asia/Ho_Chi_Minh",
    "timezone_abbreviation": "GMT+7",
    "elevation": 7.0,

    "hourly_units": {
        "time": "iso8601",
        "temperature_2m": "°C",
        "relative_humidity_2m": "%",
        "precipitation": "mm",
        "wind_speed_10m": "km/h",
        "weather_code": "wmo code",
    },

    "hourly": {
        "time": [
            "2026-08-16T03:00",
            "2026-08-16T04:00",
        ],
        "temperature_2m": [
            27.5,
            27.1,
        ],
        "relative_humidity_2m": [
            82,
            84,
        ],
        "precipitation": [
            0.0,
            0.2,
        ],
        "wind_speed_10m": [
            8.5,
            9.1,
        ],
        "weather_code": [
            1,
            2,
        ],
    },
}


fake_result = WeatherApiResult(
    payload=fake_payload,
    status_code=200,
    endpoint=(
        "https://api.open-meteo.com/v1/forecast"
    ),
    request_params={
        "latitude": 10.82,
        "longitude": 106.63,
        "hourly": (
            "temperature_2m,"
            "relative_humidity_2m,"
            "precipitation,"
            "wind_speed_10m,"
            "weather_code"
        ),
        "timezone": "Asia/Ho_Chi_Minh",
        "forecast_hours": 48,
    },
)

metadata = build_weather_ingestion_metadata(
    api_type="forecast",
    warehouse_id="WH_HCM",

    run_id="test_weather_run_001",
    ingestion_id=TEST_INGESTION_ID,

    logical_at=TEST_REQUESTED_AT,
    requested_at=TEST_REQUESTED_AT,

    api_result=fake_result,
)

service_client = get_adls_service_client()

bronze_client = (
    service_client.get_file_system_client(
        "bronze"
    )
)

response_path, metadata_path = (
    write_weather_to_bronze(
        bronze_client=bronze_client,
        api_result=fake_result,
        metadata=metadata,
    )
)


print(
    "Response path:",
    response_path,
)

print(
    "Metadata path:",
    metadata_path,
)

expected_root = (
    "weather/open_meteo/"
    "forecast/"
    "ingestion_date=2026-08-16/"
    "warehouse_id=WH_HCM/"
    f"ingestion_id={TEST_INGESTION_ID}"
)


assert response_path == (
    f"{expected_root}/response.json"
)

assert metadata_path == (
    f"{expected_root}/metadata.json"
)

response_bytes = (
    bronze_client
    .get_file_client(response_path)
    .download_file()
    .readall()
)

stored_response = json.loads(
    response_bytes.decode("utf-8")
)

assert stored_response == fake_payload

metadata_bytes = (
    bronze_client
    .get_file_client(metadata_path)
    .download_file()
    .readall()
)

stored_metadata = json.loads(
    metadata_bytes.decode("utf-8")
)

assert stored_metadata["version"] == 1

assert (
    stored_metadata["source_name"]
    == "open_meteo"
)

assert (
    stored_metadata["api_type"]
    == "forecast"
)

assert (
    stored_metadata["warehouse_id"]
    == "WH_HCM"
)

assert (
    stored_metadata["ingestion_id"]
    == TEST_INGESTION_ID
)

assert (
    stored_metadata["requested_latitude"]
    == 10.82
)

assert (
    stored_metadata["requested_longitude"]
    == 106.63
)

assert (
    stored_metadata["response_latitude"]
    == 10.790861
)

assert (
    stored_metadata["response_longitude"]
    == 106.6313
)

assert stored_metadata["http_status"] == 200
assert (
    stored_metadata["requested_at"]
    == TEST_REQUESTED_AT.isoformat()
)

retry_response_path, retry_metadata_path = (
    write_weather_to_bronze(
        bronze_client=bronze_client,
        api_result=fake_result,
        metadata=metadata,
    )
)

assert retry_response_path == response_path

assert retry_metadata_path == metadata_path

paths = list(
    bronze_client.get_paths(
        path=expected_root,
        recursive=True,
    )
)

files = [
    path
    for path in paths
    if not path.is_directory
]

file_names = {
    path.name
    for path in files
}

assert file_names == {
    response_path,
    metadata_path,
}

bronze_client.delete_directory(
    expected_root
)

print(
    "\nWeather Bronze Writer test: PASS"
)