import json

from datetime import (
    date,
    datetime,
    timedelta,
    timezone,
)
from unittest.mock import patch
from uuid import uuid4

from fastorder.storage.minio_client import (
    get_minio_client,
)

from fastorder.ingestion.api_based.weather_api_client import (
    WeatherApiResult,
)

from fastorder.ingestion.api_based.weather_bronze_writer import (
    BRONZE_BUCKET,
    build_historical_weather_ingestion_root,
)

from fastorder.ingestion.api_based.weather_ingestion_state import (
    build_historical_weather_ingestion_id,
)

from fastorder.ingestion.api_based.weather_ingestion_runner import (
    run_historical_forecast_ingestion,
)



# HELPERS


def list_objects_under_prefix(
    minio_client,
    prefix: str,
) -> set[str]:

    response = minio_client.list_objects_v2(
        Bucket=BRONZE_BUCKET,
        Prefix=prefix,
    )

    return {
        item["Key"]
        for item in response.get(
            "Contents",
            [],
        )
    }


def cleanup_prefix(
    minio_client,
    prefix: str,
) -> None:

    object_keys = list_objects_under_prefix(
        minio_client,
        prefix,
    )

    for object_key in object_keys:

        minio_client.delete_object(
            Bucket=BRONZE_BUCKET,
            Key=object_key,
        )

        print(
            f"[CLEANUP] Deleted: "
            f"{object_key}"
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
        raw_bytes = body.read()
    finally:
        body.close()

    return json.loads(
        raw_bytes.decode("utf-8")
    )



# TEST CONTEXT


#
# Dùng warehouse ID test riêng để tuyệt đối không đụng
# historical production object của WH_HCM.
#

TEST_SUFFIX = uuid4().hex[:8]

TEST_WAREHOUSE_ID = (
    f"TEST_WH_HIST_{TEST_SUFFIX}"
)

TEST_LATITUDE = 10.8231
TEST_LONGITUDE = 106.6297

TEST_START_DATE = date(
    2026,
    8,
    10,
)

TEST_END_DATE = date(
    2026,
    8,
    12,
)


FIRST_RUN_ID = (
    f"test_historical_runner_A_"
    f"{TEST_SUFFIX}"
)

SECOND_RUN_ID = (
    f"test_historical_runner_B_"
    f"{TEST_SUFFIX}"
)


FIRST_LOGICAL_AT = datetime(
    2026,
    8,
    16,
    0,
    0,
    tzinfo=timezone.utc,
)

SECOND_LOGICAL_AT = (
    FIRST_LOGICAL_AT
    + timedelta(hours=1)
)



# EXPECTED HISTORICAL IDENTITY


expected_ingestion_id = (
    build_historical_weather_ingestion_id(
        warehouse_id=TEST_WAREHOUSE_ID,
        start_date=TEST_START_DATE,
        end_date=TEST_END_DATE,
    )
)


expected_ingestion_root = (
    build_historical_weather_ingestion_root(
        warehouse_id=TEST_WAREHOUSE_ID,
        ingestion_id=expected_ingestion_id,
        start_date=TEST_START_DATE,
        end_date=TEST_END_DATE,
    )
)


expected_prefix = (
    expected_ingestion_root + "/"
)


print(
    "Expected ingestion ID:",
    expected_ingestion_id,
)

print(
    "Expected ingestion root:",
    expected_ingestion_root,
)



# FAKE OPEN-METEO HISTORICAL RESULT


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
            "2026-08-10T00:00",
            "2026-08-11T00:00",
            "2026-08-12T00:00",
        ],
        "temperature_2m": [
            27.5,
            27.1,
            26.8,
        ],
        "relative_humidity_2m": [
            82,
            84,
            85,
        ],
        "precipitation": [
            0.0,
            0.2,
            0.0,
        ],
        "wind_speed_10m": [
            8.5,
            9.1,
            7.9,
        ],
        "weather_code": [
            1,
            2,
            1,
        ],
    },
}


fake_result = WeatherApiResult(
    payload=fake_payload,
    status_code=200,
    endpoint=(
        "https://historical-forecast-api."
        "open-meteo.com/v1/forecast"
    ),
    request_params={
        "latitude": TEST_LATITUDE,
        "longitude": TEST_LONGITUDE,
        "start_date": (
            TEST_START_DATE.isoformat()
        ),
        "end_date": (
            TEST_END_DATE.isoformat()
        ),
        "timezone": "Asia/Ho_Chi_Minh",
    },
)



# REAL MINIO


minio_client = get_minio_client()


cleanup_prefix(
    minio_client,
    expected_prefix,
)


try:

    with patch(
        (
            "fastorder.ingestion.api_based."
            "weather_ingestion_runner."
            "fetch_historical_forecast"
        ),
        return_value=fake_result,
    ) as mock_fetch:


        
        # TEST 1 — FIRST RUN MUST COMMIT
        

        print(
            "\n========== RUN #1 =========="
        )

        result_1 = (
            run_historical_forecast_ingestion(
                minio_client=minio_client,

                warehouse_id=TEST_WAREHOUSE_ID,
                latitude=TEST_LATITUDE,
                longitude=TEST_LONGITUDE,

                start_date=TEST_START_DATE,
                end_date=TEST_END_DATE,

                run_id=FIRST_RUN_ID,
                logical_at=FIRST_LOGICAL_AT,
            )
        )


        print(
            "Status:",
            result_1.status,
        )

        print(
            "Ingestion ID:",
            result_1.ingestion_id,
        )

        print(
            "Root:",
            result_1.ingestion_root,
        )


        assert (
            result_1.status
            == "committed"
        )

        assert (
            result_1.ingestion_id
            == expected_ingestion_id
        )

        assert (
            result_1.ingestion_root
            == expected_ingestion_root
        )

        assert result_1.response_path == (
            f"{expected_ingestion_root}/"
            "response.json"
        )

        assert result_1.metadata_path == (
            f"{expected_ingestion_root}/"
            "metadata.json"
        )

        assert result_1.success_path == (
            f"{expected_ingestion_root}/"
            "_SUCCESS"
        )


        assert mock_fetch.call_count == 1

        mock_fetch.assert_called_once_with(
            latitude=TEST_LATITUDE,
            longitude=TEST_LONGITUDE,
            start_date=TEST_START_DATE,
            end_date=TEST_END_DATE,
        )


        
        # VERIFY 3 BRONZE OBJECTS
        

        expected_objects = {
            result_1.response_path,
            result_1.metadata_path,
            result_1.success_path,
        }

        object_keys = (
            list_objects_under_prefix(
                minio_client,
                expected_prefix,
            )
        )


        assert object_keys == expected_objects

        assert len(object_keys) == 3


        stored_response = read_json_object(
            minio_client,
            result_1.response_path,
        )

        assert (
            stored_response
            == fake_payload
        )


        stored_metadata = read_json_object(
            minio_client,
            result_1.metadata_path,
        )

        assert (
            stored_metadata["api_type"]
            == "historical_forecast"
        )

        assert (
            stored_metadata["warehouse_id"]
            == TEST_WAREHOUSE_ID
        )

        assert (
            stored_metadata["ingestion_id"]
            == expected_ingestion_id
        )

        assert (
            stored_metadata["run_id"]
            == FIRST_RUN_ID
        )

        assert (
            stored_metadata["http_status"]
            == 200
        )


        print(
            "Run #1 historical commit: PASS"
        )


        
        # TEST 2 — DIFFERENT RUN_ID, SAME WINDOW MUST SKIP
        

        print(
            "\n========== RUN #2 =========="
        )

        result_2 = (
            run_historical_forecast_ingestion(
                minio_client=minio_client,

                warehouse_id=TEST_WAREHOUSE_ID,
                latitude=TEST_LATITUDE,
                longitude=TEST_LONGITUDE,

                start_date=TEST_START_DATE,
                end_date=TEST_END_DATE,

                run_id=SECOND_RUN_ID,
                logical_at=SECOND_LOGICAL_AT,
            )
        )


        print(
            "Status:",
            result_2.status,
        )


        assert (
            result_2.status
            == "skipped"
        )

        assert (
            result_2.ingestion_id
            == expected_ingestion_id
        )

        assert (
            result_2.ingestion_root
            == expected_ingestion_root
        )

        assert (
            result_2.response_path
            is None
        )

        assert (
            result_2.metadata_path
            is None
        )

        assert result_2.success_path == (
            f"{expected_ingestion_root}/"
            "_SUCCESS"
        )


        # Quan trọng:
        # run_id đã đổi nhưng API không được gọi lại.
        assert mock_fetch.call_count == 1


        object_keys_after_replay = (
            list_objects_under_prefix(
                minio_client,
                expected_prefix,
            )
        )

        assert (
            object_keys_after_replay
            == expected_objects
        )

        assert (
            len(object_keys_after_replay)
            == 3
        )


        print(
            "Run #2 cross-run replay SKIP: PASS"
        )


    print(
        "\n"
        "HISTORICAL INGESTION RUNNER "
        "MINIO INTEGRATION: PASS"
    )


finally:

    cleanup_prefix(
        minio_client,
        expected_prefix,
    )

    print(
        "Historical runner "
        "test cleanup: PASS"
    )