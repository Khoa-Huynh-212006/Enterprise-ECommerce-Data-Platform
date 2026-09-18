from fastorder.orchestration.docker_runtime import (
    exec_in_service,
)


def validate_platform() -> None:

    exec_in_service(
        "dbt",
        [
            "dbt",
            "test",
        ],
    )
