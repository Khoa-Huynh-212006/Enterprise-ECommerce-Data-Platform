from datetime import date

from fastorder.ingestion.api_based.weather_ingestion_state import (
    build_historical_weather_ingestion_id,
)


start_date = date(
    2026,
    7,
    17,
)

end_date = date(
    2026,
    8,
    15,
)


id_1 = build_historical_weather_ingestion_id(
    warehouse_id="WH_HCM",
    start_date=start_date,
    end_date=end_date,
)

id_2 = build_historical_weather_ingestion_id(
    warehouse_id="WH_HCM",
    start_date=start_date,
    end_date=end_date,
)

id_other_window = (
    build_historical_weather_ingestion_id(
        warehouse_id="WH_HCM",
        start_date=date(
            2026,
            6,
            17,
        ),
        end_date=date(
            2026,
            7,
            16,
        ),
    )
)


print(
    "ID 1:",
    id_1,
)

print(
    "ID 2:",
    id_2,
)

print(
    "Other window:",
    id_other_window,
)


assert id_1 == id_2

assert id_1 != id_other_window


print(
    "\nHistorical ingestion identity test: PASS"
)