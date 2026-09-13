import os
from functools import reduce

from pyspark.sql import (
    DataFrame,
    SparkSession,
    functions as F,
)


STAGING_SCHEMA = "staging"


def get_jdbc_config() -> tuple[str, dict]:

    url = (
        f"jdbc:postgresql://{os.environ['DWH_DB_HOST']}:"
        f"{os.environ['DWH_DB_PORT']}/"
        f"{os.environ['DWH_DB_NAME']}"
    )

    properties = {
        "user": os.environ["DWH_DB_USER"],
        "password": os.environ["DWH_DB_PASSWORD"],
        "driver": "org.postgresql.Driver",
        "batchsize": "5000",
    }

    return url, properties


def _quote_identifier(
    identifier: str,
) -> str:

    escaped = identifier.replace(
        '"',
        '""',
    )

    return f'"{escaped}"'


def _qualified_table(
    schema: str,
    table: str,
) -> str:

    return (
        f"{_quote_identifier(schema)}."
        f"{_quote_identifier(table)}"
    )


def validate_staging(
    df: DataFrame,
    config,
    expected_count: int,
) -> None:

    actual_count = df.count()

    null_condition = reduce(
        lambda left, right: left | right,
        [
            F.col(column).isNull()
            for column
            in config.primary_key_columns
        ],
    )

    null_pk = (
        df
        .filter(null_condition)
        .count()
    )

    duplicate_pk = (
        df
        .groupBy(
            *config.primary_key_columns
        )
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    print(
        f"Expected rows: {expected_count}"
    )
    print(
        f"Staging rows: {actual_count}"
    )
    print(
        f"NULL PK: {null_pk}"
    )
    print(
        f"Duplicate PK: {duplicate_pk}"
    )

    if actual_count != expected_count:
        raise ValueError(
            "Staging row count "
            "không khớp Silver"
        )

    if (
        null_pk != 0
        or duplicate_pk != 0
    ):
        raise ValueError(
            "Staging vi phạm "
            "primary key contract"
        )

def _replace_staging_atomically(
    spark: SparkSession,
    url: str,
    properties: dict,
    load_table: str,
    target_table: str,
    columns: list[str],
) -> None:

    jvm = spark._jvm

    driver = jvm.org.postgresql.Driver()

    jdbc_properties = (
        jvm.java.util.Properties()
    )

    jdbc_properties.setProperty(
        "user",
        properties["user"],
    )

    jdbc_properties.setProperty(
        "password",
        properties["password"],
    )

    connection = driver.connect(
        url,
        jdbc_properties,
    )

    if connection is None:
        raise RuntimeError(
            "Không thể tạo JDBC connection "
            "tới PostgreSQL DWH."
        )

    statement = (
        connection.createStatement()
    )

    connection.setAutoCommit(False)

    load_relation = _qualified_table(
        STAGING_SCHEMA,
        load_table,
    )

    target_relation = _qualified_table(
        STAGING_SCHEMA,
        target_table,
    )

    column_list = ", ".join(
        _quote_identifier(column)
        for column in columns
    )

    try:
        result = statement.executeQuery(
            "SELECT EXISTS ("
            "SELECT 1 "
            "FROM information_schema.tables "
            f"WHERE table_schema = '{STAGING_SCHEMA}' "
            f"AND table_name = '{target_table}'"
            ")"
        )

        result.next()
        target_exists = result.getBoolean(1)
        result.close()

        if not target_exists:
            statement.execute(
                f"ALTER TABLE "
                f"{load_relation} "
                f"RENAME TO "
                f"{_quote_identifier(target_table)}"
            )

        else:
            statement.execute(
                f"TRUNCATE TABLE "
                f"{target_relation}"
            )

            statement.execute(
                f"INSERT INTO "
                f"{target_relation} "
                f"({column_list}) "
                f"SELECT "
                f"{column_list} "
                f"FROM {load_relation}"
            )

            statement.execute(
                f"DROP TABLE "
                f"{load_relation}"
            )

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        statement.close()
        connection.close()


def load_operational_table(
    spark: SparkSession,
    config,
) -> None:

    table = config.table_name

    silver_path = (
        f"s3a://silver/"
        f"operational/{table}"
    )

    load_table = (
        f"__load_{table}"
    )

    load_relation = (
        f"{STAGING_SCHEMA}."
        f"{load_table}"
    )

    target_relation = (
        f"{STAGING_SCHEMA}."
        f"{table}"
    )

    print(
        f"\nĐang load: {table}"
    )

    silver_df = (
        spark.read
        .format("delta")
        .load(silver_path)
    )

    silver_count = (
        silver_df.count()
    )

    print(
        f"Silver rows: {silver_count}"
    )

    url, properties = (
        get_jdbc_config()
    )


    (
        silver_df
        .coalesce(4)
        .write
        .jdbc(
            url=url,
            table=load_relation,
            mode="overwrite",
            properties=properties,
        )
    )

    load_df = (
        spark.read
        .jdbc(
            url=url,
            table=load_relation,
            properties=properties,
        )
    )

    validate_staging(
        df=load_df,
        config=config,
        expected_count=silver_count,
    )

    _replace_staging_atomically(
        spark=spark,
        url=url,
        properties=properties,
        load_table=load_table,
        target_table=table,
        columns=silver_df.columns,
    )

    staging_df = (
        spark.read
        .jdbc(
            url=url,
            table=target_relation,
            properties=properties,
        )
    )


    validate_staging(
        df=staging_df,
        config=config,
        expected_count=silver_count,
    )

    print(
        f"FASTORDER STAGING "
        f"{table.upper()}: PASS"
    )