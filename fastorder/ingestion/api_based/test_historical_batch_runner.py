from datetime import date, datetime, timezone
from unittest.mock import patch

from fastorder.ingestion.api_based.weather_ingestion_runner import (
    WeatherIngestionResult,
    run_all_historical_forecast_ingestions,
)


fake_bronze_client = object()

logical_at = datetime(
    2026,
    8,
    16,
    tzinfo=timezone.utc,
)


def fake_run_historical(**kwargs):

    return WeatherIngestionResult(
        status="committed",
        warehouse_id=kwargs["warehouse_id"],
        ingestion_id="fake-id",
        ingestion_root="fake-root",
        response_path="response.json",
        metadata_path="metadata.json",
        success_path="_SUCCESS",
    )


with patch(
    "fastorder.ingestion.api_based."
    "weather_ingestion_runner."
    "run_historical_forecast_ingestion",
    side_effect=fake_run_historical,
) as mock_run:

    result = (
        run_all_historical_forecast_ingestions(
            bronze_client=fake_bronze_client,

            end_date=date(
                2026,
                8,
                15,
            ),

            run_id="test_historical_batch",
            logical_at=logical_at,

            total_days=90,
            window_days=30,
        )
    )


assert mock_run.call_count == 15

assert result.total == 15
assert result.committed == 15
assert result.skipped == 0


print(
    "Historical batch runner test: PASS"
)