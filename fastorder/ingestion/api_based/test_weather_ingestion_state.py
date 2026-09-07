from uuid import uuid4

from fastorder.storage.minio_client import (
    get_minio_client,
)

from fastorder.ingestion.api_based.weather_ingestion_state import (
    BRONZE_BUCKET,
    build_weather_ingestion_id,
    success_marker_exists,
    write_success_marker,
)



# Helper


def cleanup_prefix(
    minio_client,
    prefix: str,
) -> None:

    response = minio_client.list_objects_v2(
        Bucket=BRONZE_BUCKET,
        Prefix=prefix,
    )

    for item in response.get(
        "Contents",
        [],
    ):
        object_key = item["Key"]

        minio_client.delete_object(
            Bucket=BRONZE_BUCKET,
            Key=object_key,
        )

        print(
            f"[CLEANUP] Deleted: "
            f"{object_key}"
        )



# TEST 1: Deterministic ingestion ID


id_1 = build_weather_ingestion_id(
    api_type="forecast",
    warehouse_id="WH_HCM",
    run_id="test_run_001",
)

id_2 = build_weather_ingestion_id(
    api_type="forecast",
    warehouse_id="WH_HCM",
    run_id="test_run_001",
)

id_3 = build_weather_ingestion_id(
    api_type="forecast",
    warehouse_id="WH_HCM",
    run_id="test_run_002",
)


print(
    "ID #1:",
    id_1,
)

print(
    "ID #2:",
    id_2,
)

print(
    "ID #3:",
    id_3,
)


assert id_1 == id_2
assert id_1 != id_3


print(
    "\nDeterministic ingestion ID test: PASS"
)



# TEST 2: _SUCCESS marker trên MinIO thật


minio_client = get_minio_client()

TEST_RUN_ID = uuid4().hex[:8]

TEST_ROOT = (
    "weather/open_meteo/"
    "test_state/"
    f"ingestion_id=test-success-marker-{TEST_RUN_ID}"
)

TEST_PREFIX = (
    TEST_ROOT + "/"
)


# Cleanup phòng trường hợp test root đã tồn tại
cleanup_prefix(
    minio_client,
    TEST_PREFIX,
)


try:


    # Trạng thái ban đầu: chưa có _SUCCESS


    assert (
        success_marker_exists(
            minio_client=minio_client,
            ingestion_root=TEST_ROOT,
        )
        is False
    )

    print(
        "_SUCCESS before commit: False"
    )



    # Commit lần đầu


    success_path = write_success_marker(
        minio_client=minio_client,
        ingestion_root=TEST_ROOT,
    )


    print(
        "Success marker path:",
        success_path,
    )


    assert success_path == (
        f"{TEST_ROOT}/_SUCCESS"
    )


    assert (
        success_marker_exists(
            minio_client=minio_client,
            ingestion_root=TEST_ROOT,
        )
        is True
    )

    print(
        "_SUCCESS after commit: True"
    )



    # Retry commit cùng logical ingestion


    retry_success_path = (
        write_success_marker(
            minio_client=minio_client,
            ingestion_root=TEST_ROOT,
        )
    )


    assert (
        retry_success_path
        == success_path
    )


    response = minio_client.list_objects_v2(
        Bucket=BRONZE_BUCKET,
        Prefix=TEST_PREFIX,
    )


    object_keys = {
        item["Key"]
        for item in response.get(
            "Contents",
            [],
        )
    }


    assert object_keys == {
        success_path,
    }


    print(
        "_SUCCESS retry overwrite: PASS"
    )


    print(
        "\n"
        "WEATHER INGESTION STATE "
        "MINIO E2E: PASS"
    )


finally:

    cleanup_prefix(
        minio_client,
        TEST_PREFIX,
    )

    print(
        "Weather ingestion state "
        "test cleanup: PASS"
    )