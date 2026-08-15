from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from airflow.sdk import dag, task

from fastorder.storage.adls_client import get_adls_service_client
from fastorder.ingestion.file_based.file_ingestion_runner import run_file_ingestion


SOURCE_ROOT = "clickstream/yoochoose/prepared"

MANIFEST_PATH = Path(
    "/opt/airflow/state/file_based/"
    "yoochoose_manifest.json"
)


@dag(
    dag_id="yoochoose_file_ingestion",
    start_date=datetime(
        2026,
        1,
        1,
        tzinfo=ZoneInfo("Asia/Ho_Chi_Minh")
    ),
    schedule=None,
    catchup=False,
    tags=[
        "fastorder",
        "file-ingestion",
        "yoochoose",
    ]
)

def yoochoose_file_ingestion():

    @task
    def ingest_files():
        service_client = get_adls_service_client()

        landing_client = (
            service_client.get_file_system_client("landing")
        )

        bronze_client = (
            service_client.get_file_system_client("bronze")
        )

        result = run_file_ingestion(
            landing_client=landing_client,
            bronze_client=bronze_client,
            source_root=SOURCE_ROOT,
            manifest_path=MANIFEST_PATH,
        )

        return {
            "discovered": result.discovered,
            "processed": result.processed,
            "skipped": result.skipped,
            "retried": result.retried,
        }

    ingest_files()


yoochoose_file_ingestion()