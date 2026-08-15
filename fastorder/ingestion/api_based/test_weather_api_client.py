from fastorder.ingestion.api_based.weather_api_client import (
    fetch_forecast,
)


payload = fetch_forecast(
    latitude=10.82,
    longitude=106.63,
)

print(
    "Latitude:",
    payload.get("latitude"),
)

print(
    "Longitude:",
    payload.get("longitude"),
)

print(
    "Timezone:",
    payload.get("timezone"),
)


hourly = payload["hourly"]

print(
    "Hourly fields:",
    list(hourly.keys()),
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


assert len(hourly["time"]) == 48

print(
    "\nOpen-Meteo Forecast API smoke test: PASS"
)

print(
    "Top-level fields:",
    list(payload.keys()),
)

print(
    "Hourly units:",
    payload.get("hourly_units"),
)

print(
    "Elevation:",
    payload.get("elevation"),
)

print(
    "UTC offset:",
    payload.get("utc_offset_seconds"),
)

print(
    "Generation time ms:",
    payload.get("generationtime_ms"),
)