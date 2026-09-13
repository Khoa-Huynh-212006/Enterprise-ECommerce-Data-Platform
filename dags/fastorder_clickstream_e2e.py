import pendulum

from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)
from airflow.sdk import dag, task

from fastorder.orchestration.clickstream_runtime import (
    build_clickstream_models,
    load_clickstream_dwh,
    run_clickstream_silver,
    test_clickstream_source,
)


@dag(
    dag_id="fastorder_clickstream_e2e",
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
        "clickstream",
        "e2e",
    ],
)
def fastorder_clickstream_e2e():

    ingest_clickstream = TriggerDagRunOperator(
        task_id="ingest_clickstream",
        trigger_dag_id="yoochoose_file_ingestion",
        wait_for_completion=True,
        poke_interval=5,
    )

    @task.python(
        task_id="clickstream_bronze_to_silver",
        pool="spark_local",
    )
    def bronze_to_silver():
        run_clickstream_silver()

    @task.python(
        task_id="clickstream_silver_to_dwh",
        pool="spark_local",
    )
    def silver_to_dwh():
        load_clickstream_dwh()

    @task.python(
        task_id="test_source",
    )
    def test_source():
        test_clickstream_source()

    @task.python(
        task_id="build_models",
    )
    def build_models():
        build_clickstream_models()

    silver_task = bronze_to_silver()
    dwh_task = silver_to_dwh()
    source_test_task = test_source()
    model_task = build_models()

    (
        ingest_clickstream
        >> silver_task
        >> dwh_task
        >> source_test_task
        >> model_task
    )


fastorder_clickstream_e2e()
