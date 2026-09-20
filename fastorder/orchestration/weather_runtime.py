from shlex import quote

from fastorder.orchestration.docker_runtime import (
    exec_in_service,
)


WEATHER_CONFIG = {
    "forecast": {
        "silver_script": (
            "/opt/fastorder-project/"
            "fastorder/transformation/silver/weather/"
            "run_forecast.py"
        ),
        "staging_source": "weather_forecast_hourly",
        "dbt_model": "fact_weather_forecast_hourly",
    },
    "historical": {
        "silver_script": (
            "/opt/fastorder-project/"
            "fastorder/transformation/silver/weather/"
            "run_history.py"
        ),
        "staging_source": "weather_historical_forecast_hourly",
        "dbt_model": "fact_weather_historical_forecast_hourly",
    },
}


DWH_RUNNER = (
    "/opt/fastorder-project/"
    "fastorder/loading/dwh/run_weather.py"
)


def _get_weather_config(
    weather_type: str,
) -> dict:
    if weather_type not in WEATHER_CONFIG:
        allowed = ", ".join(
            sorted(WEATHER_CONFIG)
        )

        raise ValueError(
            "weather_type không hợp lệ: "
            f"{weather_type}. "
            f"Allowed: {allowed}"
        )

    return WEATHER_CONFIG[weather_type]


def _spark_submit(
    script_path: str,
    args: tuple[str, ...] = (),
) -> None:
    quoted_args = " ".join(
        quote(value)
        for value in args
    )

    command = (
        'JARS=$(printf "%s," /opt/spark/ivy/manual-jars/*.jar); '
        'JARS=${JARS%,}; '
        '/opt/spark/bin/spark-submit '
        '--jars "$JARS" '
        f'{quote(script_path)}'
    )

    if quoted_args:
        command = (
            f"{command} "
            f"{quoted_args}"
        )

    exec_in_service(
        "spark",
        [
            "sh",
            "-lc",
            command,
        ],
    )


def _run_weather_silver(
    weather_type: str,
) -> None:
    config = _get_weather_config(
        weather_type
    )

    _spark_submit(
        config["silver_script"]
    )


def run_weather_forecast_silver() -> None:
    _run_weather_silver(
        "forecast"
    )


def run_weather_historical_silver() -> None:
    _run_weather_silver(
        "historical"
    )


def load_weather_dwh(
    weather_type: str,
) -> None:
    _get_weather_config(
        weather_type
    )

    _spark_submit(
        DWH_RUNNER,
        (
            "--table",
            weather_type,
        ),
    )


def test_weather_source(
    weather_type: str,
) -> None:
    config = _get_weather_config(
        weather_type
    )

    source_selector = (
        "source:staging_weather."
        f"{config['staging_source']}"
    )

    exec_in_service(
        "dbt",
        [
            "dbt",
            "test",
            "--select",
            source_selector,
        ],
    )


def build_weather_model(
    weather_type: str,
) -> None:
    config = _get_weather_config(
        weather_type
    )

    exec_in_service(
        "dbt",
        [
            "dbt",
            "build",
            "--select",
            config["dbt_model"],
            "--indirect-selection",
            "cautious",
        ],
    )
