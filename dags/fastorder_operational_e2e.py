from pendulum import datetime

from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)

from fastorder.orchestration.operational_runtime import (
    run_operational_silver,
    load_operational_dwh,
    test_all_operational_sources,
    build_operational_models,
)


OPERATIONAL_TABLES = (
    "geolocation",
    "customers",
    "warehouses",
    "orders",
    "products",
    "sellers",
    "order_items",
    "order_reviews",
    "order_payments",
    "product_category_name_translation",
    "inventory",
)


@dag(
    dag_id="fastorder_operational_e2e",
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
        "operational",
    ],
)
def fastorder_operational_e2e():

    @task.python
    def bronze_to_silver(
        table_name: str,
    ):
        run_operational_silver(
            table_name
        )

    @task.python
    def silver_to_dwh(
        table_name: str,
    ):
        load_operational_dwh(
            table_name
        )

    @task.python
    def test_sources():
        test_all_operational_sources()

    @task.python
    def build_models():
        build_operational_models()

    dwh_tasks = []

    for table_name in OPERATIONAL_TABLES:

        ingest = TriggerDagRunOperator(
            task_id=f"ingest_{table_name}",
            trigger_dag_id=(
                f"incremental_{table_name}_dag"
            ),
            wait_for_completion=True,
            deferrable=True,
            poke_interval=5,
        )

        silver = bronze_to_silver.override(
            task_id=(
                f"{table_name}_bronze_to_silver"
            ),
            pool="spark_local",
        )(
            table_name
        )

        dwh = silver_to_dwh.override(
            task_id=(
                f"{table_name}_silver_to_dwh"
            ),
            pool="spark_local",
        )(
            table_name
        )

        ingest >> silver >> dwh

        dwh_tasks.append(
            dwh
        )

    source_quality = test_sources()
    marts = build_models()

    for dwh_task in dwh_tasks:
        dwh_task >> source_quality

    source_quality >> marts


fastorder_operational_e2e()
