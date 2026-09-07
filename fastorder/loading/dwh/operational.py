import os
from functools import reduce

from pyspark.sql import DataFrame, SparkSession, functions as F


def get_jdbc_config() -> tuple[str, dict]:
    url = (
        f"jdbc:postgresql://{os.environ['DWH_DB_HOST']}:"
        f"{os.environ['DWH_DB_PORT']}/{os.environ['DWH_DB_NAME']}"
    )

    properties = {
        "user": os.environ["DWH_DB_USER"],
        "password": os.environ["DWH_DB_PASSWORD"],
        "driver": "org.postgresql.Driver",
        "batchsize": "5000",
    }

    return url, properties


def validate_staging(df: DataFrame, config, expected_count: int) -> None:
    actual_count = df.count()

    null_condition = reduce(
        lambda left, right: left | right,
        [F.col(column).isNull() for column in config.primary_key_columns],
    )

    null_pk = df.filter(null_condition).count()

    duplicate_pk = (
        df.groupBy(*config.primary_key_columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    print(f"Expected rows: {expected_count}")
    print(f"Staging rows: {actual_count}")
    print(f"NULL PK: {null_pk}")
    print(f"Duplicate PK: {duplicate_pk}")

    if actual_count != expected_count:
        raise ValueError("Staging row count không khớp Silver")

    if null_pk != 0 or duplicate_pk != 0:
        raise ValueError("Staging vi phạm primary key contract")


def load_operational_table(
    spark: SparkSession,
    config,
) -> None:

    table = config.table_name
    silver_path = f"s3a://silver/operational/{table}"
    staging_table = f"staging.{table}"

    print(f"\nĐang load: {table}")

    silver_df = spark.read.format("delta").load(silver_path)
    silver_count = silver_df.count()

    print(f"Silver rows: {silver_count}")

    url, properties = get_jdbc_config()

    (
        silver_df.coalesce(4)
        .write.jdbc(
            url=url,
            table=staging_table,
            mode="overwrite",
            properties=properties,
        )
    )

    staging_df = spark.read.jdbc(
        url=url,
        table=staging_table,
        properties=properties,
    )

    validate_staging(
        df=staging_df,
        config=config,
        expected_count=silver_count,
    )

    print(f"FASTORDER STAGING {table.upper()}: PASS")