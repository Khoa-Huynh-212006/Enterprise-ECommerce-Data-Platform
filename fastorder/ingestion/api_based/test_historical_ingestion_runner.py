from datetime import (
    date,
    datetime,
    timezone,
)

from fastorder.storage.adls_client import (
    get_adls_service_client,
)

from fastorder.ingestion.api_based.weather_ingestion_runner import (
    run_historical_forecast_ingestion,
)


service_client = (
    get_adls_service_client()
)

bronze_client = (
    service_client.get_file_system_client(
        "bronze"
    )
)


run_id = (
    "test_historical_runner_001"
)

logical_at = datetime(
    2026,
    8,
    16,
    0,
    0,
    tzinfo=timezone.utc,
)


result = run_historical_forecast_ingestion(
    bronze_client=bronze_client,

    warehouse_id="WH_HCM",

    latitude=10.8231,
    longitude=106.6297,

    start_date=date(
        2026,
        8,
        10,
    ),

    end_date=date(
        2026,
        8,
        12,
    ),

    run_id=run_id,
    logical_at=logical_at,
)


print()
print(
    "Status:",
    result.status,
)

print(
    "Ingestion ID:",
    result.ingestion_id,
)

print(
    "Root:",
    result.ingestion_root,
)

print(
    "Response:",
    result.response_path,
)

print(
    "Metadata:",
    result.metadata_path,
)

print(
    "Success:",
    result.success_path,
)