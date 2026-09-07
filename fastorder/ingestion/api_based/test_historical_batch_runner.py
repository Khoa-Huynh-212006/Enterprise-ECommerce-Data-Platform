from datetime import (
    date,
    datetime,
    timezone,
)
from unittest.mock import patch

from fastorder.ingestion.api_based.weather_ingestion_runner import (
    WeatherIngestionResult,
    run_all_historical_forecast_ingestions,
)



# TEST CONTEXT


fake_minio_client = object()

TEST_RUN_ID = (
    "test_historical_batch"
)

TEST_LOGICAL_AT = datetime(
    2026,
    8,
    16,
    tzinfo=timezone.utc,
)

TEST_END_DATE = date(
    2026,
    8,
    15,
)



# FAKE SINGLE HISTORICAL RUNNER


def fake_run_historical(
    **kwargs,
):

    # Outer runner phải forward đúng
    # dependency/context xuống single runner.
    assert (
        kwargs["minio_client"]
        is fake_minio_client
    )

    assert (
        kwargs["run_id"]
        == TEST_RUN_ID
    )

    assert (
        kwargs["logical_at"]
        == TEST_LOGICAL_AT
    )

    return WeatherIngestionResult(
        status="committed",
        warehouse_id=kwargs[
            "warehouse_id"
        ],
        ingestion_id="fake-id",
        ingestion_root="fake-root",
        response_path="response.json",
        metadata_path="metadata.json",
        success_path="_SUCCESS",
    )



# RUN BATCH


with patch(
    (
        "fastorder.ingestion.api_based."
        "weather_ingestion_runner."
        "run_historical_forecast_ingestion"
    ),
    side_effect=fake_run_historical,
) as mock_run:

    result = (
        run_all_historical_forecast_ingestions(
            minio_client=fake_minio_client,

            end_date=TEST_END_DATE,

            run_id=TEST_RUN_ID,
            logical_at=TEST_LOGICAL_AT,

            total_days=90,
            window_days=30,
        )
    )



# VALIDATION


print(
    "Single historical runner calls:",
    mock_run.call_count,
)

print(
    "Total:",
    result.total,
)

print(
    "Committed:",
    result.committed,
)

print(
    "Skipped:",
    result.skipped,
)


assert mock_run.call_count == 15

assert result.total == 15
assert result.committed == 15
assert result.skipped == 0

assert len(result.results) == 15


print(
    "\n"
    "HISTORICAL BATCH RUNNER "
    "TEST: PASS"
)