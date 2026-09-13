from pendulum import datetime

from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)

from fastorder.orchestration.docker_runtime import (
    exec_in_service,
)


SPARK_JARS = """
JARS=$(printf "%s," /opt/spark/ivy/manual-jars/*.jar)
JARS=${JARS%,}
"""

SILVER_COMMAND = f"""
{SPARK_JARS}
/opt/spark/bin/spark-submit \
    --jars "$JARS" \
    /opt/fastorder-project/fastorder/transformation/silver/operational/run_table.py \
    --table orders
"""

DWH_COMMAND = f"""
{SPARK_JARS}
/opt/spark/bin/spark-submit \
    --jars "$JARS" \
    /opt/fastorder-project/fastorder/loading/dwh/run_operational.py \
    --table orders
"""


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
        exec_in_service(
            "spark",
            [
                "sh",
                "-lc",
                SILVER_COMMAND,
            ],
        )

    @task.python
    def orders_silver_to_dwh():
        exec_in_service(
            "spark",
            [
                "sh",
                "-lc",
                DWH_COMMAND,
            ],
        )

    @task.python
    def test_orders_source():
        exec_in_service(
            "dbt",
            [
                "dbt",
                "test",
                "--select",
                "source:staging_operational.orders",
            ],
        )

    silver = orders_bronze_to_silver()
    dwh = orders_silver_to_dwh()
    dbt_test = test_orders_source()

    ingest_orders >> silver >> dwh >> dbt_test


fastorder_orders_e2e()