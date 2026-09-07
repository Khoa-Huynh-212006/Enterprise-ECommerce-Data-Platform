from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from fastorder.ingestion.api_based.weather_config import (
    WAREHOUSE_WEATHER_LOCATIONS,
)

from fastorder.ingestion.api_based.weather_ingestion_runner import (
    WeatherIngestionResult,
    run_all_forecast_ingestions,
)


TEST_RUN_ID = "test_weather_multi_run_001"

TEST_LOGICAL_AT = datetime(
    2026,
    8,
    16,
    6,
    0,
    tzinfo=timezone.utc,
)



# Fake MinIO client

#
# Outer runner không trực tiếp thao tác MinIO.
# Nó chỉ truyền minio_client xuống single runner.
#
# Vì single runner bị mock hoàn toàn trong test này,
# ta không cần kết nối MinIO thật.
#

fake_minio_client = MagicMock()



# Fake behavior cho từng warehouse


COMMITTED_WAREHOUSES = {
    "WH_HN",
    "WH_HP",
    "WH_DN",
}

SKIPPED_WAREHOUSES = {
    "WH_HCM",
    "WH_CT",
}


def fake_run_forecast_ingestion(
    *,
    minio_client,
    warehouse_id,
    latitude,
    longitude,
    run_id,
    logical_at,
):

    if warehouse_id in COMMITTED_WAREHOUSES:

        status = "committed"

        response_path = (
            f"fake/{warehouse_id}/response.json"
        )

        metadata_path = (
            f"fake/{warehouse_id}/metadata.json"
        )

    elif warehouse_id in SKIPPED_WAREHOUSES:

        status = "skipped"

        response_path = None
        metadata_path = None

    else:

        raise RuntimeError(
            f"Warehouse ngoài test config: "
            f"{warehouse_id}"
        )

    ingestion_id = (
        f"fake-ingestion-{warehouse_id}"
    )

    ingestion_root = (
        f"fake/{warehouse_id}/"
        f"ingestion_id={ingestion_id}"
    )

    success_path = (
        f"{ingestion_root}/_SUCCESS"
    )

    return WeatherIngestionResult(
        status=status,

        warehouse_id=warehouse_id,
        ingestion_id=ingestion_id,
        ingestion_root=ingestion_root,

        response_path=response_path,
        metadata_path=metadata_path,
        success_path=success_path,
    )



# Patch single-warehouse runner


with patch(
    (
        "fastorder.ingestion.api_based."
        "weather_ingestion_runner."
        "run_forecast_ingestion"
    ),
    side_effect=fake_run_forecast_ingestion,
) as mock_single_runner:

    result = run_all_forecast_ingestions(
        minio_client=fake_minio_client,
        run_id=TEST_RUN_ID,
        logical_at=TEST_LOGICAL_AT,
    )



# TEST 1: Aggregate counters


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


assert result.total == 5
assert result.committed == 3
assert result.skipped == 2
assert len(result.results) == 5


print(
    "Aggregate counters: PASS"
)



# TEST 2: Single runner phải được gọi 5 lần


assert mock_single_runner.call_count == 5


print(
    "Single runner call count: PASS"
)



# TEST 3: Đúng thứ tự warehouse config


expected_warehouse_ids = [
    warehouse.warehouse_id
    for warehouse
    in WAREHOUSE_WEATHER_LOCATIONS
]

actual_warehouse_ids = [
    item.warehouse_id
    for item
    in result.results
]


print(
    "Expected warehouses:",
    expected_warehouse_ids,
)

print(
    "Actual warehouses:",
    actual_warehouse_ids,
)


assert (
    actual_warehouse_ids
    == expected_warehouse_ids
)


print(
    "Warehouse order: PASS"
)



# TEST 4: Mỗi call nhận đúng config


for index, warehouse in enumerate(
    WAREHOUSE_WEATHER_LOCATIONS
):

    call = (
        mock_single_runner
        .call_args_list[index]
    )

    kwargs = call.kwargs

    assert (
        kwargs["minio_client"]
        is fake_minio_client
    )

    assert (
        kwargs["warehouse_id"]
        == warehouse.warehouse_id
    )

    assert (
        kwargs["latitude"]
        == warehouse.latitude
    )

    assert (
        kwargs["longitude"]
        == warehouse.longitude
    )

    assert (
        kwargs["run_id"]
        == TEST_RUN_ID
    )

    assert (
        kwargs["logical_at"]
        == TEST_LOGICAL_AT
    )


print(
    "Warehouse configuration forwarding: PASS"
)



# TEST 5: Statuses đúng


statuses = {
    item.warehouse_id: item.status
    for item in result.results
}


for warehouse_id in COMMITTED_WAREHOUSES:

    assert (
        statuses[warehouse_id]
        == "committed"
    )


for warehouse_id in SKIPPED_WAREHOUSES:

    assert (
        statuses[warehouse_id]
        == "skipped"
    )


print(
    "Warehouse statuses: PASS"
)


print(
    "\n"
    "WEATHER MULTI-INGESTION "
    "RUNNER TEST: PASS"
)