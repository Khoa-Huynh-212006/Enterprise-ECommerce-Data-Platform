import requests
from dataclasses import dataclass
from typing import Any
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import date

FORECAST_URL = (
    "https://api.open-meteo.com/v1/forecast"
)

HISTORICAL_FORECAST_URL = (
    "https://historical-forecast-api.open-meteo.com/"
    "v1/forecast"
)

HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "weather_code",
]

@dataclass(frozen=True)
class WeatherApiResult:
    payload: dict
    status_code: int
    endpoint: str
    request_params: dict[str, Any]

def build_http_session() -> requests.Session:

    retry_policy = Retry(
        total=3,
        connect=3,
        read=3,
        status=3,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET",}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry_policy)
    session = requests.Session()
    session.mount("https://", adapter,)

    return session

HTTP_SESSION = build_http_session()


def fetch_forecast(
    *,
    latitude: float,
    longitude: float,
) -> WeatherApiResult:
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

    response = HTTP_SESSION.get(
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

    if "hourly" not in payload:
        raise ValueError(
            "Open-Meteo response thiếu field 'hourly'"
        )
    
    if "hourly_units" not in payload:
        raise ValueError(
            "Open-Meteo response thiếu 'hourly_units'"
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



    return WeatherApiResult(
        payload=payload,
        status_code=response.status_code,
        endpoint=FORECAST_URL,
        request_params=params
    )

def fetch_historical_forecast(
    *,
    latitude: float,
    longitude: float,
    start_date: date,
    end_date: date,
) -> WeatherApiResult:

    if not isinstance(start_date, date):
        raise ValueError(
            "start_date phải là date"
        )

    if not isinstance(end_date, date):
        raise ValueError(
            "end_date phải là date"
        )

    if start_date > end_date:
        raise ValueError(
            "start_date không được lớn hơn end_date"
        )

    params = {
        "latitude": latitude,
        "longitude": longitude,

        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),

        "hourly": ",".join(
            HOURLY_VARIABLES
        ),

        "timezone": "Asia/Ho_Chi_Minh",
    }

    response = HTTP_SESSION.get(
        HISTORICAL_FORECAST_URL,
        params=params,
        timeout=30,
    )

    response.raise_for_status()

    payload = response.json()

    if not isinstance(payload, dict):
        raise ValueError(
            "Open-Meteo response không phải JSON object"
        )

    if "hourly" not in payload:
        raise ValueError(
            "Open-Meteo response thiếu field 'hourly'"
        )

    if "hourly_units" not in payload:
        raise ValueError(
            "Open-Meteo response thiếu 'hourly_units'"
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
                "Open-Meteo hourly response "
                "thiếu variable: "
                f"{variable}"
            )

    expected_length = len(
        hourly["time"]
    )

    for variable in HOURLY_VARIABLES:

        actual_length = len(
            hourly[variable]
        )

        if actual_length != expected_length:
            raise ValueError(
                "Open-Meteo hourly array "
                "length mismatch: "
                f"variable={variable}, "
                f"expected={expected_length}, "
                f"actual={actual_length}"
            )

    return WeatherApiResult(
        payload=payload,
        status_code=response.status_code,
        endpoint=HISTORICAL_FORECAST_URL,
        request_params=dict(params),
    )