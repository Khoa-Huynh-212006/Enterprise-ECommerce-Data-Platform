import json
import re

from pathlib import Path
from pendulum import datetime
from zoneinfo import ZoneInfo

from airflow.sdk import (
    dag,
    task,
    get_current_context,
)

from fastorder.db.connection import (
    get_engine,
)
from fastorder.ingestion.incremental.table_config import (
    get_table_config,
)
from fastorder.ingestion.incremental.incremental_runner import (
    run_table_incremental_ingestion,
)
from fastorder.orchestration.docker_runtime import (
    exec_compose_service,
)


def _sanitize_run_id(
    raw_id: str,
) -> str:
    safe_id = raw_id.replace(
        ":",
        "-",
    ).replace(
        "+",
        "_",
    )

    return re.sub(
        r'[<>"/\\|?*]',
        "_",
        safe_id,
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
    tags=[
        "fastorder",
        "e2e",
        "orders",
    ],
)
def fastorder_orders_e2e():

    @task.python(
        task_id="ingest_orders_incremental"
    )
    def ingest_orders():
        context = get_current_context()
        dag_run = context["dag_run"]

        raw_run_id = dag_run.run_id
        safe_run_id = _sanitize_run_id(
            raw_run_id
        )

        vn_start_date = (
            dag_run
            .start_date
            .astimezone(
                ZoneInfo(
                    "Asia/Ho_Chi_Minh"
                )
            )
        )

        run_started_at = (
            vn_start_date.replace(
                tzinfo=None
            )
        )

        airflow_home = Path(
            "/opt/airflow"
        )

        checkpoint_path = (
            airflow_home
            / "state"
            / "checkpoints"
            / "orders_checkpoint.json"
        )

        pending_path = (
            airflow_home
            / "state"
            / "pending"
            / "orders_pending_batch.json"
        )

        config = get_table_config(
            "orders"
        )

        engine = get_engine()

        with engine.connect() as conn:
            result = (
                run_table_incremental_ingestion(
                    conn=conn,
                    config=config,
                    checkpoint_path=checkpoint_path,
                    pending_context_path=pending_path,
                    batch_size=5000,
                    run_id=safe_run_id,
                    run_started_at=run_started_at,
                )
            )

        print(
            json.dumps(
                result,
                indent=4,
                ensure_ascii=False,
                default=str,
            )
        )

    @task.python(
        task_id="orders_bronze_to_silver"
    )
    def build_orders_silver():
        command = """
        JARS=$(printf "%s," /opt/spark/ivy/manual-jars/*.jar)
        JARS=${JARS%,}

        /opt/spark/bin/spark-submit \
          --jars "$JARS" \
          /opt/fastorder-project/fastorder/transformation/silver/operational/run_table.py \
          --table orders
        """

        exec_compose_service(
            "spark",
            command,
        )

    @task.python(
        task_id="orders_silver_to_dwh"
    )
    def load_orders_dwh():
        command = """
        JARS=$(printf "%s," /opt/spark/ivy/manual-jars/*.jar)
        JARS=${JARS%,}

        /opt/spark/bin/spark-submit \
          --jars "$JARS" \
          /opt/fastorder-project/fastorder/loading/dwh/run_operational.py \
          --table orders
        """

        exec_compose_service(
            "spark",
            command,
        )

    @task.python(
        task_id="test_orders_dwh_contract"
    )
    def test_orders_dwh_contract():
        exec_compose_service(
            "dbt",
            (
                "dbt test "
                "--select "
                "'source:staging_operational.orders'"
            ),
        )

    ingestion = ingest_orders()
    silver = build_orders_silver()
    dwh = load_orders_dwh()
    dbt_test = test_orders_dwh_contract()

    (
        ingestion
        >> silver
        >> dwh
        >> dbt_test
    )


fastorder_orders_e2e()