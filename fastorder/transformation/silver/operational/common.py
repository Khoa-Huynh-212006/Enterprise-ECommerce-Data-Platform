from functools import reduce
from operator import and_

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

from fastorder.transformation.common.spark_session import create_spark_session


def validate_bronze(df: DataFrame, config) -> None:
    required_columns = set(config.select_columns) | {"_ingested_at"}
    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        raise ValueError(f"Bronze thiếu các cột: {sorted(missing_columns)}")

    null_pk_condition = reduce(
        lambda left, right: left | right,
        [F.col(column).isNull() for column in config.primary_key_columns]
    )

    null_pk_count = df.filter(null_pk_condition).count()

    if null_pk_count > 0:
        raise ValueError(f"Bronze có {null_pk_count} dòng chứa NULL trong primary key")


def build_latest_state(df: DataFrame, config) -> DataFrame:
    validate_bronze(df, config)

    window = Window.partitionBy(*config.primary_key_columns).orderBy(
        F.col(config.watermark_column).desc_nulls_last(),
        F.col("_ingested_at").desc_nulls_last()
    )

    return (
        df.withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def delta_table_exists(spark, path: str) -> bool:
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(f"{path}/_delta_log")
    file_system = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return file_system.exists(hadoop_path)


def find_changes(spark, latest_df: DataFrame, config, silver_path: str) -> DataFrame:
    target_df = (
        spark.read.format("delta").load(silver_path)
        .select(*config.primary_key_columns, config.watermark_column, "_ingested_at")
    )

    join_conditions = [
        F.col(f"source.{column}") == F.col(f"target.{column}")
        for column in config.primary_key_columns
    ]
    join_condition = reduce(and_, join_conditions)

    watermark = config.watermark_column
    first_pk = config.primary_key_columns[0]

    change_condition = (
        F.col(f"target.{first_pk}").isNull()
        | (F.col(f"source.{watermark}") > F.col(f"target.{watermark}"))
        | (
            F.col(f"source.{watermark}").eqNullSafe(F.col(f"target.{watermark}"))
            & (F.col("source._ingested_at") > F.col("target._ingested_at"))
        )
        | (
            F.col(f"source.{watermark}").isNotNull()
            & F.col(f"target.{watermark}").isNull()
        )
    )

    return (
        latest_df.alias("source")
        .join(target_df.alias("target"), join_condition, "left")
        .filter(change_condition)
        .select("source.*")
    )


def merge_to_silver(spark, latest_df: DataFrame, config, silver_path: str) -> int:
    if not delta_table_exists(spark, silver_path):
        print("Silver chưa tồn tại, tạo Delta table mới.")
        latest_df.write.format("delta").mode("overwrite").save(silver_path)
        return latest_df.count()

    changes_df = find_changes(spark, latest_df, config, silver_path).cache()
    change_count = changes_df.count()

    print(f"Số bản ghi cần MERGE: {change_count}")

    if change_count == 0:
        print("Không có thay đổi, bỏ qua MERGE.")
        changes_df.unpersist()
        return 0

    changes_df.createOrReplaceTempView("operational_updates")

    pk_condition = " AND ".join(
        f"target.{column} = source.{column}"
        for column in config.primary_key_columns
    )

    watermark = config.watermark_column

    spark.sql(f"""
        MERGE INTO delta.`{silver_path}` AS target
        USING operational_updates AS source
        ON {pk_condition}

        WHEN MATCHED AND (
            source.{watermark} > target.{watermark}
            OR (
                source.{watermark} <=> target.{watermark}
                AND source._ingested_at > target._ingested_at
            )
            OR (
                source.{watermark} IS NOT NULL
                AND target.{watermark} IS NULL
            )
        )
        THEN UPDATE SET *

        WHEN NOT MATCHED THEN INSERT *
    """)

    changes_df.unpersist()
    return change_count


def validate_silver(spark, config, silver_path: str, expected_count: int) -> None:
    df = spark.read.format("delta").load(silver_path)

    silver_count = df.count()
    duplicate_count = (
        df.groupBy(*config.primary_key_columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    print(f"Expected rows: {expected_count}")
    print(f"Silver rows: {silver_count}")
    print(f"Duplicate PK: {duplicate_count}")

    if silver_count != expected_count:
        raise ValueError("Số dòng Silver không khớp latest state của Bronze")

    if duplicate_count != 0:
        raise ValueError("Silver có primary key bị duplicate")


def run_operational_silver(config, transform=None) -> None:
    spark = create_spark_session(app_name=f"fastorder-{config.table_name}-silver")
    spark.conf.set("spark.sql.shuffle.partitions", "8")

    bronze_path = f"s3a://bronze/{config.table_name}"
    silver_path = f"s3a://silver/operational/{config.table_name}"

    print(f"Đọc Bronze: {bronze_path}")

    bronze_df = spark.read.parquet(bronze_path)
    bronze_count = bronze_df.count()

    latest_df = build_latest_state(bronze_df, config)

    if transform is not None:
        latest_df = transform(latest_df)

    latest_df = latest_df.withColumn("_silver_processed_at", F.current_timestamp())
    latest_count = latest_df.count()

    print(f"Bronze versions: {bronze_count}")
    print(f"Latest business rows: {latest_count}")
    print(f"Historical versions: {bronze_count - latest_count}")

    merge_to_silver(spark, latest_df, config, silver_path)
    validate_silver(spark, config, silver_path, latest_count)

    spark.stop()
    print(f"FASTORDER {config.table_name.upper()} SILVER: PASS")