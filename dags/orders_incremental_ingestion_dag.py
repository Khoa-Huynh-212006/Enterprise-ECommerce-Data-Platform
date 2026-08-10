import json
import re
from pathlib import Path
from pendulum import datetime

from airflow.sdk import dag, task, get_current_context

from fastorder.db.connection import get_engine
from fastorder.ingestion.incremental.incremental_runner import run_orders_incremental_ingestion


def _sanitize_run_id(raw_id: str) -> str:
    """
    Biến đổi raw run_id của Airflow thành dạng an toàn cho Windows File System.
    Đảm bảo tính deterministic: Cùng 1 input luôn ra đúng 1 output (quan trọng cho pending recovery).
    """
    safe_id = raw_id.replace(":", "-").replace("+", "_")
    safe_id = re.sub(r'[<>"/\\|?*]', "_", safe_id)
    return safe_id


@dag(
    dag_id="incremental_orders_dag",
    start_date=datetime(year=2026, month=1, day=1, tz="Asia/Ho_Chi_Minh"),
    schedule=None,
    catchup=False, 
    max_active_runs=1,
    is_paused_upon_creation=False 
)
def incremental_orders_dag():
    
    @task.python
    def ingest_orders_incremental():

        context = get_current_context()
        dag_run = context["dag_run"]

        raw_run_id = dag_run.run_id
        safe_run_id = _sanitize_run_id(raw_run_id)
        run_started_at = dag_run.start_date.replace(tzinfo=None)

        airflow_home = Path("/opt/airflow")
        
        checkpoint_path = airflow_home / "state" / "checkpoints" / "orders_checkpoint.json"
        pending_path = airflow_home / "state" / "pending" / "orders_pending_batch.json"

        if not checkpoint_path.parent.exists():
            raise RuntimeError(f"Checkpoint directory không tồn tại: {checkpoint_path.parent}")
        if not pending_path.parent.exists():
            raise RuntimeError(f"Pending directory không tồn tại: {pending_path.parent}")

        print("Bắt đầu incremental orders ingestion task (ADLS Bronze)")
        print(f"Raw Airflow Run ID: {raw_run_id}")
        print(f"Safe Pipeline ID  : {safe_run_id}")
        print(f"Started At        : {run_started_at}")
        print(f"Checkpoint        : {checkpoint_path}")

        engine = get_engine()

        with engine.connect() as conn: 
            result = run_orders_incremental_ingestion(
                conn=conn,
                checkpoint_path=checkpoint_path,
                pending_context_path=pending_path,
                batch_size=5000,
                run_id=safe_run_id,
                run_started_at=run_started_at
            )

        print("Kết quả thực thi\n")
        print(json.dumps(result, indent=4, ensure_ascii=False, default=str))

    ingest_orders_incremental()

incremental_orders_dag()