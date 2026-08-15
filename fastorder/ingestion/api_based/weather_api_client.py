import requests
FORECAST_URL = (
    "https://api.open-meteo.com/v1/forecast"
)

HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "weather_code",
]


def fetch_forecast(
    *,
    latitude: float,
    longitude: float,
) -> dict:
    """
    Lấy dữ liệu 48 giờ từ weather forecast
    nguồn: Open-Meteo
    """

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": "Asia/Ho_Chi_Minh",
        "forecast_hours": 48,
    }

    response = requests.get(
        FORECAST_URL,
        params=params,
        timeout=30,
    )

    response.raise_for_status()

    payload = response.json()

    if not isinstance(payload, dict):
        raise ValueError(
            "Open-Meteo response không phải JSON object"
        )

    hourly = payload["hourly"]

    if not isinstance(hourly, dict):
        raise ValueError(
            "Open-Meteo field 'hourly' không phải object"
        )

    if "time" not in hourly:
        raise ValueError(
            "Open-Meteo hourly response thiếu 'time'"
        )

    for variable in HOURLY_VARIABLES:
        if variable not in hourly:
            raise ValueError(
                "Open-Meteo hourly response thiếu variable: "
                f"{variable}"
            )

    expected_length = len(hourly["time"])

    for variable in HOURLY_VARIABLES:
        actual_length = len(hourly[variable])

        if actual_length != expected_length:
            raise ValueError(
                "Open-Meteo hourly array length mismatch: "
                f"variable={variable}, "
                f"expected={expected_length}, "
                f"actual={actual_length}"
            )

    if "hourly_units" not in payload:
        raise ValueError(
            "Open-Meteo response thiếu 'hourly_units'"
        )

    if "hourly" not in payload:
        raise ValueError(
            "Open-Meteo response thiếu field 'hourly'"
        )

    return payload