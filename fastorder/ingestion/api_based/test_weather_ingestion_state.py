from fastorder.storage.adls_client import (
    get_adls_service_client,
)

from fastorder.ingestion.api_based.weather_ingestion_state import (
    build_weather_ingestion_id,
    success_marker_exists,
    write_success_marker,
)


# =========================================================
# TEST 1: Deterministic ingestion ID
# =========================================================

id_1 = build_weather_ingestion_id(
    api_type="forecast",
    warehouse_id="WH_HCM",
    run_id="test_run_001",
)

id_2 = build_weather_ingestion_id(
    api_type="forecast",
    warehouse_id="WH_HCM",
    run_id="test_run_001",
)

id_3 = build_weather_ingestion_id(
    api_type="forecast",
    warehouse_id="WH_HCM",
    run_id="test_run_002",
)


print(
    "ID #1:",
    id_1,
)

print(
    "ID #2:",
    id_2,
)

print(
    "ID #3:",
    id_3,
)


assert id_1 == id_2
assert id_1 != id_3


print(
    "\nDeterministic ingestion ID test: PASS"
)


# =========================================================
# TEST 2: _SUCCESS marker trên ADLS thật
# =========================================================

service_client = get_adls_service_client()

bronze_client = (
    service_client.get_file_system_client(
        "bronze"
    )
)


TEST_ROOT = (
    "weather/open_meteo/"
    "test_state/"
    "ingestion_id=test_success_marker_001"
)


# Cleanup artifact cũ nếu lần test trước bị crash
try:
    bronze_client.delete_directory(
        TEST_ROOT
    )
except Exception:
    pass


# ---------------------------------------------------------
# Trạng thái ban đầu: chưa có _SUCCESS
# ---------------------------------------------------------

assert (
    success_marker_exists(
        bronze_client=bronze_client,
        ingestion_root=TEST_ROOT,
    )
    is False
)

print(
    "_SUCCESS before commit: False"
)


# ---------------------------------------------------------
# Commit lần đầu
# ---------------------------------------------------------

success_path = write_success_marker(
    bronze_client=bronze_client,
    ingestion_root=TEST_ROOT,
)


print(
    "Success marker path:",
    success_path,
)


assert success_path == (
    f"{TEST_ROOT}/_SUCCESS"
)


assert (
    success_marker_exists(
        bronze_client=bronze_client,
        ingestion_root=TEST_ROOT,
    )
    is True
)

print(
    "_SUCCESS after commit: True"
)


# ---------------------------------------------------------
# Retry commit cùng logical ingestion
# ---------------------------------------------------------

retry_success_path = write_success_marker(
    bronze_client=bronze_client,
    ingestion_root=TEST_ROOT,
)


assert (
    retry_success_path
    == success_path
)


paths = list(
    bronze_client.get_paths(
        path=TEST_ROOT,
        recursive=True,
    )
)


files = [
    path
    for path in paths
    if not path.is_directory
]


assert len(files) == 1


assert files[0].name == success_path


print(
    "_SUCCESS retry overwrite: PASS"
)


# ---------------------------------------------------------
# Cleanup
# ---------------------------------------------------------

bronze_client.delete_directory(
    TEST_ROOT
)


print(
    "\nWeather ingestion state test: PASS"
)