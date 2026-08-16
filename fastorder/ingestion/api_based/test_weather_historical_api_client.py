from datetime import date

from fastorder.ingestion.api_based.weather_api_client import (
    fetch_historical_forecast,
)


result = fetch_historical_forecast(
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
)


payload = result.payload
hourly = payload["hourly"]


print(
    "HTTP status:",
    result.status_code,
)

print(
    "Endpoint:",
    result.endpoint,
)

print(
    "Timezone:",
    payload.get("timezone"),
)

print(
    "Hourly records:",
    len(hourly["time"]),
)

print(
    "First timestamp:",
    hourly["time"][0],
)

print(
    "Last timestamp:",
    hourly["time"][-1],
)


assert result.status_code == 200

assert len(hourly["time"]) == 72


print(
    "\nHistorical Forecast API smoke test: PASS"
)