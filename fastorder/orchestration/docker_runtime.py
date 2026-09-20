import socket
import subprocess


def _run_output(command: list[str]) -> str:
    result = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def get_compose_project() -> str:
    # Airflow worker tự tìm Compose project mà nó đang thuộc về.
    hostname = socket.gethostname()

    project = _run_output([
        "docker",
        "inspect",
        "--format",
        '{{ index .Config.Labels "com.docker.compose.project" }}',
        hostname,
    ])

    if not project:
        raise RuntimeError(
            "Không xác định được Docker Compose project."
        )

    return project


def get_service_container(service_name: str) -> str:
    project = get_compose_project()

    output = _run_output([
        "docker",
        "ps",
        "--filter",
        f"label=com.docker.compose.project={project}",
        "--filter",
        f"label=com.docker.compose.service={service_name}",
        "--filter",
        "status=running",
        "--format",
        "{{.ID}}",
    ])

    container_ids = [
        value.strip()
        for value in output.splitlines()
        if value.strip()
    ]

    if len(container_ids) != 1:
        raise RuntimeError(
            f"Expected đúng 1 container đang chạy cho "
            f"service '{service_name}', "
            f"nhưng tìm thấy {len(container_ids)}."
        )

    return container_ids[0]


def exec_in_service(
    service_name: str,
    command: list[str],
) -> None:
    container_id = get_service_container(
        service_name
    )

    print(
        f"Executing in service={service_name}, "
        f"container={container_id}"
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