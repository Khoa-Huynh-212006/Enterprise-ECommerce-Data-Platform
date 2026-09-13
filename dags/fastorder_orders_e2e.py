from pendulum import datetime

from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)

from fastorder.orchestration.operational_runtime import (
    run_operational_silver,
    load_operational_dwh,
    test_operational_dwh_source,
)


@dag(
    dag_id="fastorder_orders_e2e",
    start_date=datetime(
        year=2026,
        month=9,
        day=8,
        tz="Asia/Ho_Chi_Minh",
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["fastorder", "e2e", "orders"],
)
def fastorder_orders_e2e():

    ingest_orders = TriggerDagRunOperator(
        task_id="ingest_orders",
        trigger_dag_id="incremental_orders_dag",
        wait_for_completion=True,
        poke_interval=5,
    )

    @task.python
    def orders_bronze_to_silver():
        run_operational_silver(
            "orders"
        )

    @task.python
    def orders_silver_to_dwh():
        load_operational_dwh(
            "orders"
        )

    @task.python
    def test_orders_source():
        test_operational_dwh_source(
            "orders"
        )

    silver = orders_bronze_to_silver()
    dwh = orders_silver_to_dwh()
    dbt_test = test_orders_source()

    ingest_orders >> silver >> dwh >> dbt_test


fastorder_orders_e2e()