from datetime import timedelta

import pendulum

from airflow.sdk import (
    dag,
    task,
    get_current_context,
)

from fastorder.storage.minio_client import (
    get_minio_client,
)

from fastorder.ingestion.api_based.weather_ingestion_runner import (
    run_all_forecast_ingestions,
)


@dag(
    dag_id="weather_forecast_ingestion",
    start_date=pendulum.datetime(
        2026,
        8,
        16,
        tz="Asia/Ho_Chi_Minh",
    ),
    schedule=None,
    catchup=False,
    tags=[
        "fastorder",
        "api-ingestion",
        "weather",
        "open-meteo",
    ],
)
def weather_forecast_ingestion():

    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
    )
    def ingest_weather():

        context = get_current_context()

        ti = context["ti"]

        run_id = ti.run_id
        logical_at = context["logical_date"]

        print(
            f"Airflow run_id: "
            f"{run_id}"
        )

        print(
            f"Airflow logical_at: "
            f"{logical_at}"
        )

        minio_client = (
            get_minio_client()
        )

        result = (
            run_all_forecast_ingestions(
                minio_client=minio_client,
                run_id=run_id,
                logical_at=logical_at,
            )
        )

        print(
            "\nWeather Forecast Summary"
        )

        print(
            f"Total: {result.total}"
        )

        print(
            f"Committed: {result.committed}"
        )

        print(
            f"Skipped: {result.skipped}"
        )

        return {
            "total": result.total,
            "committed": result.committed,
            "skipped": result.skipped,
        }

    ingest_weather()


weather_forecast_ingestion()