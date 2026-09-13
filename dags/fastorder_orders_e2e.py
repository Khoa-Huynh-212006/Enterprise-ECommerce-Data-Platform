from pendulum import datetime

from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)

from fastorder.orchestration.operational_runtime import (
    run_operational_silver,
    load_operational_dwh,
    test_operational_dwh_sources,
    build_orders_mart,
)


ORDER_DOMAIN_TABLES = (
    "orders",
    "order_items",
    "order_payments",
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

    ingest_order_items = TriggerDagRunOperator(
        task_id="ingest_order_items",
        trigger_dag_id="incremental_order_items_dag",
        wait_for_completion=True,
        poke_interval=5,
    )

    ingest_order_payments = TriggerDagRunOperator(
        task_id="ingest_order_payments",
        trigger_dag_id="incremental_order_payments_dag",
        wait_for_completion=True,
        poke_interval=5,
    )

    @task.python
    def orders_bronze_to_silver():
        run_operational_silver(
            "orders"
        )

    @task.python
    def order_items_bronze_to_silver():
        run_operational_silver(
            "order_items"
        )

    @task.python
    def order_payments_bronze_to_silver():
        run_operational_silver(
            "order_payments"
        )

    @task.python
    def orders_silver_to_dwh():
        load_operational_dwh(
            "orders"
        )

    @task.python
    def order_items_silver_to_dwh():
        load_operational_dwh(
            "order_items"
        )

    @task.python
    def order_payments_silver_to_dwh():
        load_operational_dwh(
            "order_payments"
        )

    @task.python
    def source_quality_gate():
        test_operational_dwh_sources(
            ORDER_DOMAIN_TABLES
        )

    @task.python
    def build_order_analytics():
        build_orders_mart()

    orders_silver = orders_bronze_to_silver()
    items_silver = order_items_bronze_to_silver()
    payments_silver = order_payments_bronze_to_silver()

    orders_dwh = orders_silver_to_dwh()
    items_dwh = order_items_silver_to_dwh()
    payments_dwh = order_payments_silver_to_dwh()

    quality_gate = source_quality_gate()
    analytics = build_order_analytics()

    (
        [
            ingest_orders,
            ingest_order_items,
            ingest_order_payments,
        ]
        >> orders_silver
        >> orders_dwh
        >> items_silver
        >> items_dwh
        >> payments_silver
        >> payments_dwh
        >> quality_gate
        >> analytics
    )


fastorder_orders_e2e()