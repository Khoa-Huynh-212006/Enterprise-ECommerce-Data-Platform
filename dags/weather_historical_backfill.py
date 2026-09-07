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
    run_all_historical_forecast_ingestions,
)


@dag(
    dag_id="weather_historical_backfill",

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
        "weather",
        "historical",
        "backfill",
    ],
)
def weather_historical_backfill():

    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
    )
    def ingest_historical_weather():

        context = get_current_context()

        ti = context["ti"]

        run_id = ti.run_id
        logical_at = context["logical_date"]

        print(
            f"Airflow run_id: {run_id}"
        )

        print(
            f"Airflow logical_at: {logical_at}"
        )



        # Historical bootstrap boundary


        logical_date_vn = (
            logical_at.in_timezone(
                "Asia/Ho_Chi_Minh"
            )
        )

        end_date = (
            logical_date_vn
            .subtract(days=1)
            .date()
        )


        print(
            "Historical bootstrap end_date: "
            f"{end_date}"
        )



        # MinIO


        minio_client = (
            get_minio_client()
        )



        # Historical backfill


        result = (
            run_all_historical_forecast_ingestions(
                minio_client=minio_client,
                end_date=end_date,
                run_id=run_id,
                logical_at=logical_at,
                total_days=90,
                window_days=30,
            )
        )



        # Summary


        print(
            "\nHistorical backfill completed"
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


    ingest_historical_weather()


weather_historical_backfill()