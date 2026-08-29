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

from fastorder.db.connection import get_engine

from fastorder.ingestion.incremental.table_config import (
    get_table_config,
)

from fastorder.ingestion.incremental.incremental_runner import (
    run_table_incremental_ingestion,
)

def _sanitize_run_id(raw_id: str) -> str:
    """
    Chuyển Airflow run_id thành path-safe,
    deterministic pipeline run ID.

    Cùng input phải luôn tạo cùng output để
    preserve stable identity khi recovery.
    """
    safe_id = raw_id.replace(":", "-").replace("+", "_")
    safe_id = re.sub(r'[<>"/\\|?*]', "_", safe_id)
    return safe_id

def build_incremental_table_dag(
    table_name: str,
    batch_size: int = 5000,
):

    dag_id = (
        f"incremental_{table_name}_dag"
    )

    @dag(
        dag_id=dag_id,
        start_date=datetime(
            year=2026,
            month=1,
            day=1,
            tz="Asia/Ho_Chi_Minh",
        ),
        schedule=None,
        catchup=False,
        max_active_runs=1,
        is_paused_upon_creation=False,
    )
    def table_incremental_dag():

        @task.python(
            task_id=(
                f"ingest_{table_name}"
                "_incremental"
            )
        )
        def ingest_table_incremental():

            context = get_current_context()

            dag_run = context["dag_run"]

            raw_run_id = dag_run.run_id

            safe_run_id = (
                _sanitize_run_id(
                    raw_run_id
                )
            )

            vn_start_date = (
                dag_run.start_date.astimezone(
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
                / (
                    f"{table_name}"
                    "_checkpoint.json"
                )
            )

            pending_path = (
                airflow_home
                / "state"
                / "pending"
                / (
                    f"{table_name}"
                    "_pending_batch.json"
                )
            )

            if (
                not checkpoint_path
                .parent
                .exists()
            ):
                raise RuntimeError(
                    "Checkpoint directory "
                    "không tồn tại: "
                    f"{checkpoint_path.parent}"
                )

            if (
                not pending_path
                .parent
                .exists()
            ):
                raise RuntimeError(
                    "Pending directory "
                    "không tồn tại: "
                    f"{pending_path.parent}"
                )


            config = get_table_config(
                table_name
            )

            print(
                "Bắt đầu incremental "
                f"{table_name} ingestion "
                "task (ADLS Bronze)"
            )

            print(
                f"Source Table      : "
                f"{table_name}"
            )

            print(
                f"Raw Airflow Run ID: "
                f"{raw_run_id}"
            )

            print(
                f"Safe Pipeline ID  : "
                f"{safe_run_id}"
            )

            print(
                f"Started At        : "
                f"{run_started_at}"
            )

            print(
                f"Checkpoint        : "
                f"{checkpoint_path}"
            )

            print(
                f"Pending           : "
                f"{pending_path}"
            )

            print(
                f"Batch Size        : "
                f"{batch_size}"
            )

            engine = get_engine()

            with engine.connect() as conn:

                result = (
                    run_table_incremental_ingestion(
                        conn=conn,
                        config=config,
                        checkpoint_path=
                            checkpoint_path,
                        pending_context_path=
                            pending_path,
                        batch_size=
                            batch_size,
                        run_id=
                            safe_run_id,
                        run_started_at=
                            run_started_at,
                    )
                )

            print(
                "Kết quả thực thi\n"
            )

            print(
                json.dumps(
                    result,
                    indent=4,
                    ensure_ascii=False,
                    default=str,
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

            return result


        ingest_table_incremental()

    return table_incremental_dag()                        
              
incremental_orders_dag = (
    build_incremental_table_dag(
        table_name="orders",
        batch_size=5000,
    )
)

incremental_customers_dag = (
    build_incremental_table_dag(
        table_name="customers",
        batch_size=5000,
    )
)

incremental_warehouses_dag = (
    build_incremental_table_dag(
        table_name="warehouses",
        batch_size=5000,
    )
)

incremental_product_category_name_translation_dag = (
    build_incremental_table_dag(
        table_name="product_category_name_translation",
        batch_size=5000,
    )
)

incremental_sellers_dag = (
    build_incremental_table_dag(
        table_name="sellers",
        batch_size=5000,
    )
)

incremental_products_dag = (
    build_incremental_table_dag(
        table_name="products",
        batch_size=5000,
    )
)

incremental_geolocation_dag = (
    build_incremental_table_dag(
        table_name="geolocation",
        batch_size=5000,
    )
)


incremental_order_items_dag = (
    build_incremental_table_dag(
        table_name="order_items",
        batch_size=5000,
    )
)


incremental_order_payments_dag = (
    build_incremental_table_dag(
        table_name="order_payments",
        batch_size=5000,
    )
)


incremental_order_reviews_dag = (
    build_incremental_table_dag(
        table_name="order_reviews",
        batch_size=5000,
    )
)