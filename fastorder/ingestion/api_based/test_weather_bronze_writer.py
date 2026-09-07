import json

from datetime import datetime, timezone
from uuid import uuid4

from fastorder.storage.minio_client import (
    get_minio_client,
)

from fastorder.ingestion.api_based.weather_api_client import (
    WeatherApiResult,
)

from fastorder.ingestion.api_based.weather_ingestion_metadata import (
    build_weather_ingestion_metadata,
)

from fastorder.ingestion.api_based.weather_bronze_writer import (
    BRONZE_BUCKET,
    write_weather_to_bronze,
)


TEST_RUN_ID = uuid4().hex[:8]

TEST_INGESTION_ID = (
    f"test-weather-bronze-writer-{TEST_RUN_ID}"
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


def read_json_object(
    minio_client,
    object_key: str,
) -> dict:

    response = minio_client.get_object(
        Bucket=BRONZE_BUCKET,
        Key=object_key,
    )

    body = response["Body"]

    try:
        object_bytes = body.read()
    finally:
        body.close()

    return json.loads(
        object_bytes.decode("utf-8")
    )


def cleanup_prefix(
    minio_client,
    prefix: str,
) -> None:

    response = minio_client.list_objects_v2(
        Bucket=BRONZE_BUCKET,
        Prefix=prefix,
    )

    for item in response.get(
        "Contents",
        [],
    ):
        object_key = item["Key"]

        minio_client.delete_object(
            Bucket=BRONZE_BUCKET,
            Key=object_key,
        )

        print(
            f"[CLEANUP] Deleted: "
            f"{object_key}"
        )


minio_client = get_minio_client()


expected_root = (
    "weather/open_meteo/"
    "forecast/"
    "ingestion_date=2026-08-16/"
    "warehouse_id=WH_HCM/"
    f"ingestion_id={TEST_INGESTION_ID}"
)

expected_prefix = (
    expected_root + "/"
)


try:

    response_path, metadata_path = (
        write_weather_to_bronze(
            minio_client=minio_client,
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

    assert response_path == (
        f"{expected_root}/response.json"
    )

    assert metadata_path == (
        f"{expected_root}/metadata.json"
    )

    print(
        "Forecast Bronze path contract: PASS"
    )

    stored_response = read_json_object(
        minio_client,
        response_path,
    )

    assert stored_response == fake_payload

    print(
        "Raw response preservation: PASS"
    )

    stored_metadata = read_json_object(
        minio_client,
        metadata_path,
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

    assert (
        stored_metadata["http_status"]
        == 200
    )

    assert (
        stored_metadata["requested_at"]
        == TEST_REQUESTED_AT.isoformat()
    )

    print(
        "Weather metadata contract: PASS"
    )

    retry_response_path, retry_metadata_path = (
        write_weather_to_bronze(
            minio_client=minio_client,
            api_result=fake_result,
            metadata=metadata,
        )
    )

    assert (
        retry_response_path
        == response_path
    )

    assert (
        retry_metadata_path
        == metadata_path
    )

    print(
        "Retry returned same paths: PASS"
    )

    response = minio_client.list_objects_v2(
        Bucket=BRONZE_BUCKET,
        Prefix=expected_prefix,
    )

    object_keys = {
        item["Key"]
        for item in response.get(
            "Contents",
            [],
        )
    }

    assert object_keys == {
        response_path,
        metadata_path,
    }

    print(
        "Retry duplicate validation: PASS"
    )

    print(
        "\n"
        "WEATHER BRONZE WRITER "
        "MINIO E2E: PASS"
    )

finally:

    cleanup_prefix(
        minio_client,
        expected_prefix,
    )

    print(
        "Weather Bronze Writer "
        "test cleanup: PASS"
    )