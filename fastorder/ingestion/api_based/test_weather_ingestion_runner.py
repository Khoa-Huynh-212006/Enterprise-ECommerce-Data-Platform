from datetime import datetime, timezone
from unittest.mock import patch

from fastorder.storage.adls_client import (
    get_adls_service_client,
)

from fastorder.ingestion.api_based.weather_api_client import (
    WeatherApiResult,
)

from fastorder.ingestion.api_based.weather_bronze_writer import (
    build_weather_ingestion_root,
)

from fastorder.ingestion.api_based.weather_ingestion_state import (
    build_weather_ingestion_id,
)

from fastorder.ingestion.api_based.weather_ingestion_runner import (
    run_forecast_ingestion,
)


# =========================================================
# TEST CONTEXT
# =========================================================

TEST_WAREHOUSE_ID = "WH_HCM"

TEST_RUN_ID = (
    "test_weather_ingestion_runner_001"
)

TEST_LOGICAL_AT = datetime(
    2026,
    8,
    15,
    20,
    0,
    tzinfo=timezone.utc,
)

TEST_LATITUDE = 10.82
TEST_LONGITUDE = 106.63


# =========================================================
# FAKE OPEN-METEO RESULT
# =========================================================

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
        "latitude": TEST_LATITUDE,
        "longitude": TEST_LONGITUDE,
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


# =========================================================
# BUILD EXPECTED IDENTITY
# =========================================================

expected_ingestion_id = (
    build_weather_ingestion_id(
        api_type="forecast",
        warehouse_id=TEST_WAREHOUSE_ID,
        run_id=TEST_RUN_ID,
    )
)


expected_ingestion_root = (
    build_weather_ingestion_root(
        api_type="forecast",
        warehouse_id=TEST_WAREHOUSE_ID,
        ingestion_id=expected_ingestion_id,
        logical_at=TEST_LOGICAL_AT,
    )
)


print(
    "Expected ingestion ID:",
    expected_ingestion_id,
)

print(
    "Expected ingestion root:",
    expected_ingestion_root,
)


# =========================================================
# CONNECT REAL ADLS BRONZE
# =========================================================

service_client = get_adls_service_client()

bronze_client = (
    service_client.get_file_system_client(
        "bronze"
    )
)


# =========================================================
# CLEANUP OLD TEST ARTIFACT
# =========================================================

try:
    bronze_client.delete_directory(
        expected_ingestion_root
    )
except Exception:
    pass


try:

    # =====================================================
    # MOCK fetch_forecast
    # =====================================================

    with patch(
        (
            "fastorder.ingestion.api_based."
            "weather_ingestion_runner.fetch_forecast"
        ),
        return_value=fake_result,
    ) as mock_fetch:


        # =================================================
        # TEST 1: FIRST RUN MUST COMMIT
        # =================================================

        print(
            "\n========== RUN #1 =========="
        )

        result_1 = run_forecast_ingestion(
            bronze_client=bronze_client,

            warehouse_id=TEST_WAREHOUSE_ID,
            latitude=TEST_LATITUDE,
            longitude=TEST_LONGITUDE,

            run_id=TEST_RUN_ID,
            logical_at=TEST_LOGICAL_AT,
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
            "Response path:",
            result_1.response_path,
        )

        print(
            "Metadata path:",
            result_1.metadata_path,
        )

        print(
            "Success path:",
            result_1.success_path,
        )


        assert result_1.status == "committed"

        assert (
            result_1.warehouse_id
            == TEST_WAREHOUSE_ID
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


        # API phải được gọi đúng 1 lần
        assert mock_fetch.call_count == 1

        mock_fetch.assert_called_once_with(
            latitude=TEST_LATITUDE,
            longitude=TEST_LONGITUDE,
        )


        # =================================================
        # VERIFY BRONZE FILES
        # =================================================

        paths_after_run_1 = list(
            bronze_client.get_paths(
                path=expected_ingestion_root,
                recursive=True,
            )
        )

        files_after_run_1 = [
            path
            for path in paths_after_run_1
            if not path.is_directory
        ]


        file_names_after_run_1 = {
            path.name
            for path in files_after_run_1
        }


        expected_files = {
            (
                f"{expected_ingestion_root}/"
                "response.json"
            ),
            (
                f"{expected_ingestion_root}/"
                "metadata.json"
            ),
            (
                f"{expected_ingestion_root}/"
                "_SUCCESS"
            ),
        }


        assert len(files_after_run_1) == 3

        assert (
            file_names_after_run_1
            == expected_files
        )


        print(
            "Run #1 commit validation: PASS"
        )


        # =================================================
        # TEST 2: SAME LOGICAL RUN MUST SKIP
        # =================================================

        print(
            "\n========== RUN #2 =========="
        )

        result_2 = run_forecast_ingestion(
            bronze_client=bronze_client,

            warehouse_id=TEST_WAREHOUSE_ID,
            latitude=TEST_LATITUDE,
            longitude=TEST_LONGITUDE,

            run_id=TEST_RUN_ID,
            logical_at=TEST_LOGICAL_AT,
        )


        print(
            "Status:",
            result_2.status,
        )


        assert result_2.status == "skipped"

        assert (
            result_2.ingestion_id
            == expected_ingestion_id
        )

        assert (
            result_2.ingestion_root
            == expected_ingestion_root
        )

        assert result_2.response_path is None

        assert result_2.metadata_path is None

        assert result_2.success_path == (
            f"{expected_ingestion_root}/"
            "_SUCCESS"
        )


        # =================================================
        # QUAN TRỌNG:
        # Run #2 KHÔNG được gọi API lại
        # =================================================

        assert mock_fetch.call_count == 1


        print(
            "Run #2 API skip validation: PASS"
        )


        # =================================================
        # VERIFY KHÔNG SINH THÊM FILE
        # =================================================

        paths_after_run_2 = list(
            bronze_client.get_paths(
                path=expected_ingestion_root,
                recursive=True,
            )
        )

        files_after_run_2 = [
            path
            for path in paths_after_run_2
            if not path.is_directory
        ]


        file_names_after_run_2 = {
            path.name
            for path in files_after_run_2
        }


        assert len(files_after_run_2) == 3

        assert (
            file_names_after_run_2
            == expected_files
        )


        print(
            "Run #2 Bronze idempotency: PASS"
        )


    print(
        "\nWeather ingestion runner test: PASS"
    )


finally:

    # =====================================================
    # CLEANUP
    # =====================================================

    try:
        bronze_client.delete_directory(
            expected_ingestion_root
        )

        print(
            "Test Bronze cleanup: PASS"
        )

    except Exception as error:

        print(
            "WARNING: cleanup failed:",
            error,
        )