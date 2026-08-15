from fastorder.ingestion.api_based.weather_ingestion_state import (
    build_weather_ingestion_id,
)


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