import pendulum

from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)
from airflow.sdk import dag


@dag(
    dag_id="fastorder_platform_e2e",
    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="Asia/Ho_Chi_Minh",
    ),
    schedule=None,
    catchup=False,
    tags=[
        "fastorder",
        "platform",
        "e2e",
    ],
)
def fastorder_platform_e2e():

    operational = TriggerDagRunOperator(
        task_id="operational_domain",
        trigger_dag_id="fastorder_operational_e2e",
        wait_for_completion=True,
        deferrable=True,
        poke_interval=5,
    )

    weather_forecast = TriggerDagRunOperator(
        task_id="weather_forecast_domain",
        trigger_dag_id="fastorder_weather_forecast_e2e",
        wait_for_completion=True,
        deferrable=True,
        poke_interval=5,
    )

    clickstream = TriggerDagRunOperator(
        task_id="clickstream_domain",
        trigger_dag_id="fastorder_clickstream_e2e",
        wait_for_completion=True,
        deferrable=True,
        poke_interval=5,
    )

    [
        operational,
        weather_forecast,
        clickstream,
    ]


fastorder_platform_e2e()
