import subprocess


def get_running_service_container(service_name: str) -> str:
    result = subprocess.run(
        [
            "docker",
            "ps",
            "--filter",
            f"label=com.docker.compose.service={service_name}",
            "--filter",
            "status=running",
            "--format",
            "{{.ID}}",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    container_ids = [
        line.strip()
        for line in result.stdout.splitlines()
        if line.strip()
    ]

    if len(container_ids) != 1:
        raise RuntimeError(
            f"Expected exactly 1 running container for "
            f"service '{service_name}', found {len(container_ids)}."
        )

    return container_ids[0]


def run_in_service(
    service_name: str,
    command: list[str],
) -> None:
    container_id = get_running_service_container(
        service_name
    )

    print(
        f"Executing in service '{service_name}' "
        f"(container={container_id[:12]})"
    )

    subprocess.run(
        [
            "docker",
            "exec",
            container_id,
            *command,
        ],
        check=True,
    )