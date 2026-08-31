import os

import boto3
from botocore.client import BaseClient
from botocore.config import Config


def get_minio_client() -> BaseClient:
    """
    Tạo S3 client dùng để giao tiếp với MinIO.

    MinIO cung cấp S3-compatible API nên FastOrder
    sử dụng boto3 thay vì phụ thuộc MinIO-specific SDK.
    """

    endpoint = os.getenv(
        "MINIO_ENDPOINT"
    )

    access_key = os.getenv(
        "MINIO_ROOT_USER"
    )

    secret_key = os.getenv(
        "MINIO_ROOT_PASSWORD"
    )


    missing_variables = [
        variable_name
        for variable_name, variable_value
        in {
            "MINIO_ENDPOINT":
                endpoint,

            "MINIO_ROOT_USER":
                access_key,

            "MINIO_ROOT_PASSWORD":
                secret_key,
        }.items()
        if not variable_value
    ]


    if missing_variables:
        raise RuntimeError(
            "Thiếu MinIO environment variables: "
            f"{missing_variables}"
        )


    return boto3.client(
        "s3",

        endpoint_url=endpoint,

        aws_access_key_id=access_key,

        aws_secret_access_key=secret_key,

        region_name="us-east-1",

        config=Config(
            signature_version="s3v4",
            s3={
                "addressing_style":
                    "path"
            },
        ),
    )