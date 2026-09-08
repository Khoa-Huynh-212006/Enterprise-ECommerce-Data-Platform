import sys

import docker


def _get_running_service_container(
    client: docker.DockerClient,
    service: str,
):
    containers = client.containers.list(
        filters={
            "label": (
                "com.docker.compose.service="
                f"{service}"
            ),
            "status": "running",
        }
    )

    if not containers:
        raise RuntimeError(
            f"Không tìm thấy container đang chạy "
            f"cho service '{service}'."
        )

    if len(containers) > 1:
        raise RuntimeError(
            f"Tìm thấy nhiều container cho "
            f"service '{service}'."
        )

    return containers[0]


def exec_compose_service(
    service: str,
    command: str,
) -> None:
    client = docker.from_env()

    try:
        container = _get_running_service_container(
            client=client,
            service=service,
        )

        print(
            f"Running in service={service}, "
            f"container={container.name}"
        )

        exec_id = client.api.exec_create(
            container=container.id,
            cmd=["sh", "-lc", command],
            stdout=True,
            stderr=True,
        )["Id"]

        output_stream = client.api.exec_start(
            exec_id=exec_id,
            stream=True,
            demux=True,
        )

        for stdout, stderr in output_stream:
            if stdout:
                print(
                    stdout.decode(
                        "utf-8",
                        errors="replace",
                    ),
                    end="",
                )

            if stderr:
                print(
                    stderr.decode(
                        "utf-8",
                        errors="replace",
                    ),
                    end="",
                    file=sys.stderr,
                )

        # Exit code chỉ đáng tin sau khi stream đã kết thúc.
        result = client.api.exec_inspect(
            exec_id
        )

        exit_code = result["ExitCode"]

        if exit_code != 0:
            raise RuntimeError(
                f"Command failed in '{service}' "
                f"with exit code {exit_code}."
            )

    finally:
        client.close()