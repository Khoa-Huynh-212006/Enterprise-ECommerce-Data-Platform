from pendulum import datetime

from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)

from fastorder.orchestration.weather_runtime import (
    run_weather_forecast_silver,
    load_weather_dwh,
    test_weather_source,
    build_weather_model,
)


@dag(
    dag_id="fastorder_weather_forecast_e2e",
    start_date=datetime(
        year=2026,
        month=9,
        day=13,
        tz="Asia/Ho_Chi_Minh",
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=[
        "fastorder",
        "e2e",
        "weather",
        "forecast",
    ],
)
def fastorder_weather_forecast_e2e():

    @task.python
    def bronze_to_silver():
        run_weather_forecast_silver()

    @task.python
    def silver_to_dwh():
        load_weather_dwh(
            "forecast"
        )

    @task.python
    def test_source():
        test_weather_source(
            "forecast"
        )

    @task.python
    def build_model():
        build_weather_model(
            "forecast"
        )

    ingest = TriggerDagRunOperator(
        task_id="ingest_forecast",
        trigger_dag_id="weather_forecast_ingestion",
        wait_for_completion=True,
        poke_interval=5,
    )

    silver = bronze_to_silver.override(
        task_id="forecast_bronze_to_silver",
        pool="spark_local",
    )()

    dwh = silver_to_dwh.override(
        task_id="forecast_silver_to_dwh",
        pool="spark_local",
    )()

    source_quality = test_source()
    marts = build_model()

    (
        ingest
        >> silver
        >> dwh
        >> source_quality
        >> marts
    )


fastorder_weather_forecast_e2e()
