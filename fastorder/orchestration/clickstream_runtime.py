from fastorder.orchestration.docker_runtime import (
    exec_in_service,
)


SILVER_SCRIPT = (
    "/opt/fastorder-project/"
    "fastorder/transformation/silver/clickstream/"
    "yoochoose.py"
)

DWH_SCRIPT = (
    "/opt/fastorder-project/"
    "fastorder/loading/dwh/"
    "clickstream.py"
)


def _spark_submit(
    script_path: str,
) -> None:

    command = (
        'JARS=$(printf "%s," /opt/spark/ivy/manual-jars/*.jar); '
        'JARS=${JARS%,}; '
        '/opt/spark/bin/spark-submit '
        '--jars "$JARS" '
        f'{script_path}'
    )

    exec_in_service(
        "spark",
        [
            "sh",
            "-lc",
            command,
        ],
    )


def run_clickstream_silver() -> None:

    _spark_submit(
        SILVER_SCRIPT
    )


def load_clickstream_dwh() -> None:

    _spark_submit(
        DWH_SCRIPT
    )


def test_clickstream_source() -> None:

    exec_in_service(
        "dbt",
        [
            "dbt",
            "test",
            "--select",
            "source:staging_clickstream",
        ],
    )


def build_clickstream_models() -> None:

    exec_in_service(
        "dbt",
        [
            "dbt",
            "build",
            "--select",
            "path:models/intermediate/clickstream",
            "path:models/marts/clickstream",
            "--indirect-selection",
            "cautious",
        ],
    )
