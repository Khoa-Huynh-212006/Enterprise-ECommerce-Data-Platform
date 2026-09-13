import time

from fastorder.loading.dwh.operational import (
    get_jdbc_config,
)
from fastorder.transformation.common.spark_session import (
    create_spark_session,
)


SILVER_PATH = "s3a://silver/clickstream/yoochoose"

STAGING_TABLE = "staging.yoochoose_clicks"

DWH_COLUMNS = (
    "session_id",
    "event_timestamp",
    "event_date",
    "item_id",
    "category",
)


def staging_table_exists(
    spark,
    url: str,
    properties: dict,
) -> bool:
    query = """
    (
        SELECT
            to_regclass(
                'staging.yoochoose_clicks'
            ) IS NOT NULL AS table_exists
    ) q
    """

    row = (
        spark.read.jdbc(
            url=url,
            table=query,
            properties=properties,
        )
        .first()
    )

    return bool(
        row["table_exists"]
    )


def get_staging_count(
    spark,
    url: str,
    properties: dict,
) -> int:
    query = (
        "(SELECT COUNT(*) AS row_count "
        f"FROM {STAGING_TABLE}) q"
    )

    return int(
        spark.read.jdbc(
            url=url,
            table=query,
            properties=properties,
        )
        .first()["row_count"]
    )


def main() -> None:
    spark = create_spark_session(
        "fastorder-yoochoose-dwh-load"
    )

    url, properties = (
        get_jdbc_config()
    )

    df = (
        spark.read
        .format("delta")
        .load(SILVER_PATH)
        .select(*DWH_COLUMNS)
    )

    silver_count = df.count()

    input_partitions = (
        df.rdd.getNumPartitions()
    )

    print(
        f"Silver events: {silver_count}"
    )

    print(
        f"Input partitions: {input_partitions}"
    )

    if staging_table_exists(
        spark,
        url,
        properties,
    ):
        staging_count = (
            get_staging_count(
                spark,
                url,
                properties,
            )
        )

        print(
            "Existing staging events: "
            f"{staging_count}"
        )

        if staging_count == silver_count:
            print(
                "NO_OP: staging.yoochoose_clicks "
                "đã đồng bộ với Silver."
            )

            spark.stop()

            print(
                "FASTORDER YOOCHOOSE "
                "→ POSTGRES DWH: PASS"
            )

            return

        raise RuntimeError(
            "staging.yoochoose_clicks đã tồn tại "
            "nhưng row count không khớp Silver. "
            "Từ chối overwrite tự động để tránh "
            "phá downstream dbt dependencies."
        )

    print(
        "Bắt đầu bootstrap PostgreSQL staging..."
    )

    started_at = time.time()

    (
        df.coalesce(4)
        .write.jdbc(
            url=url,
            table=STAGING_TABLE,
            mode="overwrite",
            properties=properties,
        )
    )

    elapsed = (
        time.time()
        - started_at
    )

    staging_count = (
        get_staging_count(
            spark,
            url,
            properties,
        )
    )

    print(
        f"Staging events: {staging_count}"
    )

    print(
        f"Load time: {elapsed:.2f} seconds"
    )

    if staging_count != silver_count:
        raise ValueError(
            "Row count mismatch: "
            f"Silver={silver_count}, "
            f"Staging={staging_count}"
        )

    spark.stop()

    print(
        "FASTORDER YOOCHOOSE "
        "→ POSTGRES DWH: PASS"
    )


if __name__ == "__main__":
    main()
