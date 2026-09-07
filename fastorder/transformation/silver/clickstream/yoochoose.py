from pyspark.sql import DataFrame, functions as F

from fastorder.transformation.common.spark_session import create_spark_session


BRONZE_PATH = "s3a://bronze/clickstream/yoochoose"
SILVER_PATH = "s3a://silver/clickstream/yoochoose"

LINEAGE_COLUMNS = [
    "_source_name",
    "_source_file_path",
    "_source_file_etag",
    "_source_file_last_modified",
    "_source_file_size",
    "_ingestion_id",
    "_ingested_at",
    "_ingestion_method",
]


def delta_table_exists(spark, path: str) -> bool:
    log_path = spark._jvm.org.apache.hadoop.fs.Path(f"{path}/_delta_log")
    fs = log_path.getFileSystem(spark._jsc.hadoopConfiguration())

    if not fs.exists(log_path):
        return False

    for status in fs.listStatus(log_path):
        if not status.isFile():
            continue

        name = status.getPath().getName()

        # Delta hợp lệ phải có transaction log hoặc checkpoint.
        if name.endswith(".json") or ".checkpoint." in name:
            return True

    return False


def validate_bronze_schema(df: DataFrame) -> None:
    required = {"session_id", "event_timestamp", "item_id", "category", *LINEAGE_COLUMNS}
    missing = required - set(df.columns)

    if missing:
        raise ValueError(f"YOOCHOOSE Bronze thiếu các cột: {sorted(missing)}")


def build_silver_events(df: DataFrame) -> DataFrame:
    validate_bronze_schema(df)

    return (
        df.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withColumn("session_id", F.col("session_id").cast("long"))
        .withColumn("item_id", F.col("item_id").cast("long"))
        .withColumn("category", F.trim(F.col("category").cast("string")))
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("event_hour", F.hour("event_timestamp"))
        .withColumn(
            "_source_event_date",
            F.to_date(
                F.regexp_extract(
                    "_source_file_path",
                    r"event_date=(\d{4}-\d{2}-\d{2})",
                    1,
                )
            ),
        )
        .select(
            "session_id",
            "event_timestamp",
            "event_date",
            "event_hour",
            "item_id",
            "category",
            *LINEAGE_COLUMNS,
            "_source_event_date",
        )
    )


def validate_events(df: DataFrame) -> int:
    stats = df.agg(
        F.count("*").alias("rows"),
        F.sum(F.when(F.col("session_id").isNull(), 1).otherwise(0)).alias("null_session"),
        F.sum(F.when(F.col("event_timestamp").isNull(), 1).otherwise(0)).alias("null_timestamp"),
        F.sum(F.when(F.col("item_id").isNull(), 1).otherwise(0)).alias("null_item"),
        F.sum(
            F.when(
                F.col("category").isNull() | (F.col("category") == ""),
                1,
            ).otherwise(0)
        ).alias("invalid_category"),
        F.sum(
            F.when(
                F.col("event_date") != F.col("_source_event_date"),
                1,
            ).otherwise(0)
        ).alias("date_mismatch"),
    ).first()

    print(f"Events: {stats['rows']}")
    print(f"NULL session_id: {stats['null_session']}")
    print(f"NULL event_timestamp: {stats['null_timestamp']}")
    print(f"NULL item_id: {stats['null_item']}")
    print(f"Category không hợp lệ: {stats['invalid_category']}")
    print(f"Event date lệch source file: {stats['date_mismatch']}")

    invalid = (
        stats["null_session"]
        + stats["null_timestamp"]
        + stats["null_item"]
        + stats["invalid_category"]
        + stats["date_mismatch"]
    )

    if invalid != 0:
        raise ValueError(f"YOOCHOOSE Silver validation thất bại: {invalid} lỗi")

    return stats["rows"]


def get_new_sources(spark, bronze_df: DataFrame) -> DataFrame:
    bronze_sources = bronze_df.select("_source_file_path", "_source_file_etag").distinct()

    if not delta_table_exists(spark, SILVER_PATH):
        return bronze_sources

    silver_sources = (
        spark.read.format("delta").load(SILVER_PATH)
        .select("_source_file_path", "_source_file_etag")
        .distinct()
    )

    # Với cùng một path, ETag không được tự thay đổi vì source này được xem là immutable.
    changed_sources = (
        bronze_sources.alias("b")
        .join(silver_sources.alias("s"), "_source_file_path", "inner")
        .filter(F.col("b._source_file_etag") != F.col("s._source_file_etag"))
    )

    if changed_sources.limit(1).count() > 0:
        raise ValueError("Phát hiện source file cũ có ETag mới. Không append để tránh duplicate.")

    silver_paths = silver_sources.select("_source_file_path").distinct()
    return bronze_sources.join(silver_paths, "_source_file_path", "left_anti")


def validate_silver(spark, expected_rows: int) -> None:
    df = spark.read.format("delta").load(SILVER_PATH)

    silver_rows = df.count()
    source_files = df.select("_source_file_path").distinct().count()
    min_max = df.agg(F.min("event_date").alias("min_date"), F.max("event_date").alias("max_date")).first()

    print(f"Expected rows: {expected_rows}")
    print(f"Silver rows: {silver_rows}")
    print(f"Source files: {source_files}")
    print(f"Event range: {min_max['min_date']} → {min_max['max_date']}")

    if silver_rows != expected_rows:
        raise ValueError("Số event Silver không khớp Bronze")


def main() -> None:
    spark = create_spark_session("fastorder-yoochoose-silver")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.shuffle.partitions", "128")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")

    bronze_df = spark.read.parquet(BRONZE_PATH)
    bronze_count = bronze_df.count()

    print(f"Bronze events: {bronze_count}")

    new_sources = get_new_sources(spark, bronze_df).cache()
    new_source_count = new_sources.count()

    print(f"Source files mới: {new_source_count}")

    if new_source_count == 0:
        print("Không có source file mới, bỏ qua ghi Silver.")
        new_sources.unpersist()
        validate_silver(spark, bronze_count)
        spark.stop()
        print("FASTORDER YOOCHOOSE SILVER: PASS")
        return

    new_bronze_df = bronze_df.join(F.broadcast(new_sources), ["_source_file_path", "_source_file_etag"], "inner")
    silver_df = build_silver_events(new_bronze_df)

    validate_events(silver_df)

    output_df = (
        silver_df.drop("_source_event_date")
        .repartition(128, "event_date")
    )

    mode = "append" if delta_table_exists(spark, SILVER_PATH) else "overwrite"

    (
        output_df.write.format("delta")
        .mode(mode)
        .partitionBy("event_date")
        .option("maxRecordsPerFile", 500000)
        .save(SILVER_PATH)
    )

    new_sources.unpersist()
    validate_silver(spark, bronze_count)

    spark.stop()
    print("FASTORDER YOOCHOOSE SILVER: PASS")


if __name__ == "__main__":
    main()