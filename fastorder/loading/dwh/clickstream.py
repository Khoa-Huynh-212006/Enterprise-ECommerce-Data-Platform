from fastorder.loading.dwh.operational import (
    get_jdbc_config,
    _replace_staging_atomically,
)
from fastorder.transformation.common.spark_session import (
    create_spark_session,
)


SILVER_PATH = "s3a://silver/clickstream/yoochoose"

STAGING_SCHEMA = "staging"

TARGET_TABLE = "yoochoose_clicks"
LOAD_TABLE = "__load_yoochoose_clicks"

TARGET_RELATION = f"{STAGING_SCHEMA}.{TARGET_TABLE}"
LOAD_RELATION = f"{STAGING_SCHEMA}.{LOAD_TABLE}"

DWH_COLUMNS = (
    "session_id",
    "event_timestamp",
    "event_date",
    "item_id",
    "category",
)


def relation_exists(
    spark,
    url: str,
    properties: dict,
    relation: str,
) -> bool:

    query = f"""
    (
        SELECT
            to_regclass('{relation}') IS NOT NULL
            AS relation_exists
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

    return bool(row["relation_exists"])


def get_relation_quality(
    spark,
    url: str,
    properties: dict,
    relation: str,
) -> dict:

    query = f"""
    (
        SELECT
            COUNT(*) AS row_count,

            COUNT(*) FILTER (
                WHERE session_id IS NULL
            ) AS null_session_id,

            COUNT(*) FILTER (
                WHERE event_timestamp IS NULL
            ) AS null_event_timestamp,

            COUNT(*) FILTER (
                WHERE event_date IS NULL
            ) AS null_event_date,

            COUNT(*) FILTER (
                WHERE item_id IS NULL
            ) AS null_item_id,

            COUNT(*) FILTER (
                WHERE category IS NULL
                   OR category = ''
            ) AS invalid_category

        FROM {relation}
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

    return {
        "row_count": int(row["row_count"]),
        "null_session_id": int(
            row["null_session_id"]
        ),
        "null_event_timestamp": int(
            row["null_event_timestamp"]
        ),
        "null_event_date": int(
            row["null_event_date"]
        ),
        "null_item_id": int(
            row["null_item_id"]
        ),
        "invalid_category": int(
            row["invalid_category"]
        ),
    }


def validate_relation(
    spark,
    url: str,
    properties: dict,
    relation: str,
    expected_count: int,
) -> None:

    quality = get_relation_quality(
        spark,
        url,
        properties,
        relation,
    )

    print(
        f"Relation: {relation}"
    )

    print(
        f"Expected rows: {expected_count}"
    )

    print(
        f"Actual rows: {quality['row_count']}"
    )

    print(
        "NULL session_id: "
        f"{quality['null_session_id']}"
    )

    print(
        "NULL event_timestamp: "
        f"{quality['null_event_timestamp']}"
    )

    print(
        "NULL event_date: "
        f"{quality['null_event_date']}"
    )

    print(
        "NULL item_id: "
        f"{quality['null_item_id']}"
    )

    print(
        "Invalid category: "
        f"{quality['invalid_category']}"
    )

    if quality["row_count"] != expected_count:
        raise ValueError(
            "Clickstream row count không khớp Silver."
        )

    invalid = {
        key: value
        for key, value in quality.items()
        if key != "row_count"
        and value != 0
    }

    if invalid:
        raise ValueError(
            "Clickstream staging vi phạm "
            f"data-quality contract: {invalid}"
        )


def main() -> None:

    spark = create_spark_session(
        "fastorder-yoochoose-dwh-load"
    )

    try:
        url, properties = get_jdbc_config()

        silver_df = (
            spark.read
            .format("delta")
            .load(SILVER_PATH)
            .select(*DWH_COLUMNS)
        )

        silver_count = silver_df.count()

        print(
            f"Silver events: {silver_count}"
        )

        if relation_exists(
            spark,
            url,
            properties,
            TARGET_RELATION,
        ):
            target_quality = (
                get_relation_quality(
                    spark,
                    url,
                    properties,
                    TARGET_RELATION,
                )
            )

            if (
                target_quality["row_count"]
                == silver_count
            ):
                validate_relation(
                    spark=spark,
                    url=url,
                    properties=properties,
                    relation=TARGET_RELATION,
                    expected_count=silver_count,
                )

                print(
                    "NO_OP: Clickstream staging "
                    "đã đồng bộ với Silver."
                )

                print(
                    "FASTORDER YOOCHOOSE "
                    "→ POSTGRES DWH: PASS"
                )

                return

        print(
            "Bắt đầu ghi disposable "
            "load table..."
        )

        (
            silver_df
            .coalesce(4)
            .write
            .jdbc(
                url=url,
                table=LOAD_RELATION,
                mode="overwrite",
                properties=properties,
            )
        )

        validate_relation(
            spark=spark,
            url=url,
            properties=properties,
            relation=LOAD_RELATION,
            expected_count=silver_count,
        )

        print(
            "Load table PASS. "
            "Bắt đầu atomic promotion..."
        )

        _replace_staging_atomically(
            spark=spark,
            url=url,
            properties=properties,
            load_table=LOAD_TABLE,
            target_table=TARGET_TABLE,
            columns=list(DWH_COLUMNS),
        )

        validate_relation(
            spark=spark,
            url=url,
            properties=properties,
            relation=TARGET_RELATION,
            expected_count=silver_count,
        )

        if relation_exists(
            spark,
            url,
            properties,
            LOAD_RELATION,
        ):
            raise RuntimeError(
                "Load table vẫn tồn tại "
                "sau atomic promotion."
            )

        print(
            "FASTORDER YOOCHOOSE "
            "→ POSTGRES DWH: PASS"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
