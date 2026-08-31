import io
from datetime import datetime

import pyarrow.parquet as pq

from fastorder.ingestion.incremental.adls_bronze_writer import (
    _build_arrow_table,
)
from fastorder.storage.adls_client import (
    get_bronze_file_system_client,
)

from azure.core.exceptions import ResourceExistsError

def main() -> None:

    test_time = datetime.now()

    extraction_id = (
        "spark_compatibility_"
        + test_time.strftime(
            "%Y%m%d%H%M%S%f"
        )
    )

    records = [
        {
            "order_id":
                "TEST_SPARK_001",

            "customer_id":
                "TEST_CUSTOMER_001",

            # Cố tình toàn NULL để kiểm tra
            # physical type vẫn là string.
            "warehouse_id":
                None,

            "order_status":
                "delivered",

            "order_purchase_timestamp":
                datetime(
                    2026,
                    8,
                    30,
                    10,
                    0,
                    0,
                    123456,
                ),

            "order_approved_at":
                None,

            "order_delivered_carrier_date":
                None,

            "order_delivered_customer_date":
                None,

            "order_estimated_delivery_date":
                None,

            "source_system":
                "compatibility_test",

            "created_at":
                datetime(
                    2026,
                    8,
                    30,
                    10,
                    0,
                    0,
                    123456,
                ),

            "updated_at":
                datetime(
                    2026,
                    8,
                    30,
                    10,
                    0,
                    0,
                    123456,
                ),
        }
    ]

    arrow_table = _build_arrow_table(
        records=records,
        table_name="orders",
        extraction_id=extraction_id,
        ingested_at=test_time,
    )

    buffer = io.BytesIO()

    pq.write_table(
        arrow_table,
        buffer,
    )

    buffer.seek(0)

    remote_path = (
        "__integration_tests__/"
        "operational_bronze_spark_compatibility/"
        f"extraction_id={extraction_id}/"
        "part-000.parquet"
    )

    fs_client = (
        get_bronze_file_system_client()
    )


    test_root_directory = (
        "__integration_tests__"
    )

    test_suite_directory = (
        "__integration_tests__/"
        "operational_bronze_spark_compatibility"
    )

    test_extraction_directory = (
        f"{test_suite_directory}/"
        f"extraction_id={extraction_id}"
    )


    directories = [
        test_root_directory,
        test_suite_directory,
        test_extraction_directory,
    ]


    for directory_path in directories:

        directory_client = (
            fs_client.get_directory_client(
                directory_path
            )
        )

        try:
            directory_client.create_directory()

            print(
                "[CREATE DIRECTORY]",
                directory_path
            )

        except ResourceExistsError:

            print(
                "[DIRECTORY EXISTS]",
                directory_path
            )


    file_client = (
        fs_client.get_file_client(
            remote_path
        )
    )

    file_client.upload_data(
        data=buffer,
        overwrite=True,
    )

    properties = (
        file_client.get_file_properties()
    )

    if properties.size == 0:
        raise RuntimeError(
            "Test Parquet được upload "
            "nhưng có kích thước 0 bytes."
        )

    print(
        "\n[PASS] Test Parquet uploaded"
    )

    print(
        f"REMOTE_PATH={remote_path}"
    )

    print(
        f"SIZE_BYTES={properties.size}"
    )


if __name__ == "__main__":
    main()