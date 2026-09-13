import subprocess


def get_compose_service_container(service_name: str) -> str:
    result = subprocess.run(
        [
            "docker",
            "ps",
            "--filter",
            f"label=com.docker.compose.service={service_name}",
            "--format",
            "{{.ID}}",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    container_ids = [
        container_id.strip()
        for container_id in result.stdout.splitlines()
        if container_id.strip()
    ]

    if len(container_ids) != 1:
        raise RuntimeError(
            f"Expected exactly 1 running container "
            f"for service '{service_name}', "
            f"found {len(container_ids)}."
        )

    return container_ids[0]


def run_in_compose_service(
    service_name: str,
    command: str,
) -> None:
    container_id = get_compose_service_container(
        service_name
    )

    subprocess.run(
        [
            "docker",
            "exec",
            container_id,
            "sh",
            "-lc",
            command,
        ],
        check=True,
    )