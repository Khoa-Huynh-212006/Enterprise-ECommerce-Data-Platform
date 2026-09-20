from pendulum import datetime

from airflow.sdk import dag
from airflow.providers.docker.operators.docker import DockerOperator


@dag(
    dag_id="fastorder_docker_smoke",
    start_date=datetime(
        year=2026,
        month=9,
        day=8,
        tz="Asia/Ho_Chi_Minh",
    ),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["fastorder", "smoke"],
)
def fastorder_docker_smoke():

    DockerOperator(
        task_id="docker_smoke",
        image="redis:7.2-bookworm",
        command='sh -c "echo FASTORDER_DOCKER_OPERATOR_PASS"',
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        force_pull=False,
    )


fastorder_docker_smoke()