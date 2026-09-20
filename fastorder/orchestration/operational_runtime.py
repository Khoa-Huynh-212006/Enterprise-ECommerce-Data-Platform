from shlex import quote

from fastorder.ingestion.incremental.table_config import (
    get_table_config,
)
from fastorder.orchestration.docker_runtime import (
    exec_in_service,
)


def _validate_table(table_name: str) -> None:
    get_table_config(table_name)


def _spark_submit(
    script_path: str,
    args: str,
) -> None:

    command = (
        'JARS=$(printf "%s," /opt/spark/ivy/manual-jars/*.jar); '
        'JARS=${JARS%,}; '
        f'/opt/spark/bin/spark-submit '
        f'--jars "$JARS" '
        f'{quote(script_path)} '
        f'{args}'
    )

    exec_in_service(
        "spark",
        [
            "sh",
            "-lc",
            command,
        ],
    )


def run_operational_silver(
    table_name: str,
) -> None:

    _validate_table(table_name)

    _spark_submit(
        "/opt/fastorder-project/"
        "fastorder/transformation/silver/"
        "operational/run_table.py",
        f"--table {quote(table_name)}",
    )


def load_operational_dwh(
    table_name: str,
) -> None:

    _validate_table(table_name)

    _spark_submit(
        "/opt/fastorder-project/"
        "fastorder/loading/dwh/"
        "run_operational.py",
        f"--table {quote(table_name)}",
    )


def test_operational_dwh_source(
    table_name: str,
) -> None:

    _validate_table(table_name)

    exec_in_service(
        "dbt",
        [
            "dbt",
            "test",
            "--select",
            f"source:staging_operational.{table_name}",
        ],
    )


def test_operational_dwh_sources(
    table_names: tuple[str, ...],
) -> None:

    for table_name in table_names:
        _validate_table(table_name)

    selectors = [
        f"source:staging_operational.{table_name}"
        for table_name in table_names
    ]

    exec_in_service(
        "dbt",
        [
            "dbt",
            "test",
            "--select",
            *selectors,
        ],
    )


def build_orders_mart() -> None:

    exec_in_service(
        "dbt",
        [
            "dbt",
            "build",
            "--select",
            "dim_date",
            "+fact_orders",
            "--indirect-selection",
            "cautious",
        ],
    )


def test_all_operational_sources() -> None:

    exec_in_service(
        "dbt",
        [
            "dbt",
            "test",
            "--select",
            "source:staging_operational",
        ],
    )


def build_operational_models() -> None:

    exec_in_service(
        "dbt",
        [
            "dbt",
            "build",
            "--select",
            "path:models/intermediate/operational",
            "path:models/marts/core",
            "--indirect-selection",
            "cautious",
        ],
    )