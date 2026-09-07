from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window


BRONZE_ROOT = "s3a://bronze/weather/open_meteo/historical_forecast"
SILVER_PATH = "s3a://silver/weather/open_meteo/historical_forecast_hourly"
CONTROL_PATH = "s3a://silver/weather/open_meteo/_control/historical_forecast_processed"


def delta_table_exists(spark: SparkSession, path: str) -> bool:
    delta_log = spark._jvm.org.apache.hadoop.fs.Path(f"{path}/_delta_log")
    fs = delta_log.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs.exists(delta_log)


def discover_committed_ingestions(spark: SparkSession, root: str) -> list[str]:
    path = spark._jvm.org.apache.hadoop.fs.Path(root)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    files = fs.listFiles(path, True)

    committed = []
    while files.hasNext():
        file_path = files.next().getPath().toString()
        if file_path.endswith("/_SUCCESS"):
            committed.append(file_path.removesuffix("/_SUCCESS"))

    return sorted(committed)


def extract_ingestion_id(path: str) -> str:
    return path.rstrip("/").split("/")[-1].removeprefix("ingestion_id=")


def get_processed_ingestion_ids(spark: SparkSession) -> set[str]:
    if not delta_table_exists(spark, CONTROL_PATH):
        return set()

    rows = (
        spark.read.format("delta").load(CONTROL_PATH)
        .select("ingestion_id").distinct().collect()
    )

    return {row["ingestion_id"] for row in rows if row["ingestion_id"] is not None}


def find_pending_ingestions(committed: list[str], processed: set[str]) -> list[str]:
    return [path for path in committed if extract_ingestion_id(path) not in processed]


def load_pending_bronze(
    spark: SparkSession,
    pending_paths: list[str],
) -> tuple[DataFrame, DataFrame]:

    response_paths = [f"{path}/response.json" for path in pending_paths]
    metadata_paths = [f"{path}/metadata.json" for path in pending_paths]

    response_df = spark.read.option("multiline", True).json(response_paths)
    metadata_df = spark.read.option("multiline", True).json(metadata_paths)

    return response_df, metadata_df


def transform_history_hourly(response_df: DataFrame, metadata_df: DataFrame) -> DataFrame:
    response_df = (
        response_df
        .withColumn("_source_file_path", F.col("_metadata.file_path"))
        .withColumn(
            "ingestion_id",
            F.regexp_extract("_source_file_path", r"ingestion_id=([^/]+)", 1),
        )
        .withColumn(
            "_hourly",
            F.arrays_zip(
                "hourly.time",
                "hourly.temperature_2m",
                "hourly.relative_humidity_2m",
                "hourly.precipitation",
                "hourly.wind_speed_10m",
                "hourly.weather_code",
            ),
        )
        .withColumn("_hour", F.explode("_hourly"))
        .select(
            "ingestion_id",
            F.col("latitude").alias("response_latitude"),
            F.col("longitude").alias("response_longitude"),
            F.col("_hour.time").alias("weather_time_local"),
            F.col("_hour.temperature_2m").alias("temperature_2m"),
            F.col("_hour.relative_humidity_2m").alias("relative_humidity_2m"),
            F.col("_hour.precipitation").alias("precipitation"),
            F.col("_hour.wind_speed_10m").alias("wind_speed_10m"),
            F.col("_hour.weather_code").alias("weather_code"),
        )
    )

    metadata_df = metadata_df.select(
        "ingestion_id",
        "warehouse_id",
        F.col("requested_at").alias("retrieved_at"),
        F.col("request_params.start_date").alias("window_start"),
        F.col("request_params.end_date").alias("window_end"),
        "requested_latitude",
        "requested_longitude",
    )

    return (
        response_df.join(metadata_df, "ingestion_id", "left")
        .withColumn("retrieved_at", F.col("retrieved_at").cast("timestamp"))
        .withColumn("window_start", F.to_date("window_start"))
        .withColumn("window_end", F.to_date("window_end"))
        .withColumn(
            "weather_time",
            F.to_utc_timestamp(
                F.col("weather_time_local").cast("timestamp"),
                "Asia/Ho_Chi_Minh",
            ),
        )
        .drop("weather_time_local")
        .select(
            "warehouse_id",
            "weather_time",
            "ingestion_id",
            "retrieved_at",
            "window_start",
            "window_end",
            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "wind_speed_10m",
            "weather_code",
            "requested_latitude",
            "requested_longitude",
            "response_latitude",
            "response_longitude",
        )
    )


def validate_data_quality(df: DataFrame) -> None:
    stats = df.agg(
        F.sum(F.col("warehouse_id").isNull().cast("int")).alias("null_warehouse"),
        F.sum(F.col("ingestion_id").isNull().cast("int")).alias("null_ingestion"),
        F.sum(F.col("weather_time").isNull().cast("int")).alias("null_time"),
        F.sum(F.col("retrieved_at").isNull().cast("int")).alias("null_retrieved"),
        F.sum(F.col("window_start").isNull().cast("int")).alias("null_start"),
        F.sum(F.col("window_end").isNull().cast("int")).alias("null_end"),
        F.sum(F.col("temperature_2m").isNull().cast("int")).alias("null_temp"),
        F.sum(F.col("relative_humidity_2m").isNull().cast("int")).alias("null_humidity"),
        F.sum(F.col("precipitation").isNull().cast("int")).alias("null_precip"),
        F.sum(F.col("wind_speed_10m").isNull().cast("int")).alias("null_wind"),
        F.sum(F.col("weather_code").isNull().cast("int")).alias("null_code"),
        F.sum((F.col("relative_humidity_2m") < 0).cast("int")).alias("humidity_below_0"),
        F.sum((F.col("relative_humidity_2m") > 100).cast("int")).alias("humidity_above_100"),
        F.sum((F.col("precipitation") < 0).cast("int")).alias("negative_precip"),
        F.sum((F.col("wind_speed_10m") < 0).cast("int")).alias("negative_wind"),
        F.sum((F.col("window_start") > F.col("window_end")).cast("int")).alias("invalid_window"),
    ).first().asDict()

    failed = {name: value for name, value in stats.items() if value != 0}

    if failed:
        raise ValueError(f"Historical Forecast DQ thất bại: {failed}")


def validate_ingestions(df: DataFrame, metadata_df: DataFrame, pending_paths: list[str]) -> int:
    expected_ids = {extract_ingestion_id(path) for path in pending_paths}

    expected = (
        metadata_df.select(
            "ingestion_id",
            F.to_date("request_params.start_date").alias("window_start"),
            F.to_date("request_params.end_date").alias("window_end"),
        )
        .dropDuplicates(["ingestion_id"])
        .withColumn(
            "expected_rows",
            (F.datediff("window_end", "window_start") + F.lit(1)) * F.lit(24),
        )
    )

    actual = (
        df.groupBy("ingestion_id")
        .agg(
            F.count("*").alias("actual_rows"),
            F.countDistinct("weather_time").alias("distinct_hours"),
        )
    )

    metadata_ids = {
        row["ingestion_id"]
        for row in expected.select("ingestion_id").collect()
    }

    actual_ids = {
        row["ingestion_id"]
        for row in actual.select("ingestion_id").collect()
    }

    comparison = actual.join(
        expected.select("ingestion_id", "expected_rows"),
        "ingestion_id",
        "left",
    )

    invalid_rows = (
        comparison
        .filter(
            (F.col("actual_rows") != F.col("expected_rows"))
            | (F.col("distinct_hours") != F.col("expected_rows"))
        )
        .count()
    )

    expected_total = (
        expected.agg(F.sum("expected_rows").alias("rows")).first()["rows"] or 0
    )

    actual_total = df.count()

    failures = {
        "missing_metadata": len(expected_ids - metadata_ids),
        "missing_ingestions": len(expected_ids - actual_ids),
        "unexpected_ingestions": len(actual_ids - expected_ids),
        "invalid_row_counts": invalid_rows,
        "total_row_mismatch": int(expected_total != actual_total),
    }

    failed = {name: value for name, value in failures.items() if value != 0}

    print(f"Expected rows: {expected_total}")
    print(f"Actual rows: {actual_total}")

    if failed:
        raise ValueError(f"Historical Forecast validation thất bại: {failed}")

    return actual_total


def resolve_overlaps(df: DataFrame) -> DataFrame:
    weather_fields = [
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "wind_speed_10m",
        "weather_code",
    ]

    conflicts = (
        df.groupBy("warehouse_id", "weather_time")
        .agg(
            F.countDistinct(
                F.struct(*[F.col(column) for column in weather_fields])
            ).alias("versions")
        )
        .filter(F.col("versions") > 1)
        .count()
    )

    if conflicts > 0:
        raise ValueError(f"Historical overlap có {conflicts} grain chứa weather values khác nhau")

    window = (
        Window.partitionBy("warehouse_id", "weather_time")
        .orderBy(F.col("retrieved_at").desc(), F.col("ingestion_id").desc())
    )

    return (
        df.withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


def merge_silver(spark: SparkSession, df: DataFrame) -> None:
    if not delta_table_exists(spark, SILVER_PATH):
        df.write.format("delta").mode("overwrite").save(SILVER_PATH)
        return

    df.createOrReplaceTempView("history_updates")

    spark.sql(f"""
        MERGE INTO delta.`{SILVER_PATH}` AS target
        USING history_updates AS source
        ON target.warehouse_id = source.warehouse_id
        AND target.weather_time = source.weather_time

        WHEN MATCHED AND source.retrieved_at > target.retrieved_at
        THEN UPDATE SET *

        WHEN NOT MATCHED THEN INSERT *
    """)


def mark_processed(spark: SparkSession, metadata_df: DataFrame) -> None:
    processed_df = (
        metadata_df.select(
            "ingestion_id",
            "warehouse_id",
            F.to_date("request_params.start_date").alias("window_start"),
            F.to_date("request_params.end_date").alias("window_end"),
            F.col("requested_at").cast("timestamp").alias("retrieved_at"),
        )
        .dropDuplicates(["ingestion_id"])
        .withColumn("silver_processed_at", F.current_timestamp())
    )

    if not delta_table_exists(spark, CONTROL_PATH):
        processed_df.write.format("delta").mode("overwrite").save(CONTROL_PATH)
        return

    processed_df.createOrReplaceTempView("history_processed_updates")

    spark.sql(f"""
        MERGE INTO delta.`{CONTROL_PATH}` AS target
        USING history_processed_updates AS source
        ON target.ingestion_id = source.ingestion_id

        WHEN NOT MATCHED THEN INSERT *
    """)


def validate_silver(spark: SparkSession) -> None:
    df = spark.read.format("delta").load(SILVER_PATH)

    rows = df.count()
    duplicates = (
        df.groupBy("warehouse_id", "weather_time")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    print(f"Silver rows: {rows}")
    print(f"Duplicate business grain: {duplicates}")

    if duplicates != 0:
        raise ValueError("Historical Silver có duplicate business grain")


def run_history_silver(spark: SparkSession) -> dict:
    committed = discover_committed_ingestions(spark, BRONZE_ROOT)
    processed = get_processed_ingestion_ids(spark)
    pending = find_pending_ingestions(committed, processed)

    print(f"Committed ingestions: {len(committed)}")
    print(f"Processed ingestions: {len(processed)}")
    print(f"Pending ingestions: {len(pending)}")

    if not pending:
        print("Không có Historical ingestion mới, bỏ qua ghi Silver.")
        validate_silver(spark)
        return {"status": "NO_OP", "written_rows": 0}

    response_df, metadata_df = load_pending_bronze(spark, pending)
    candidate_df = transform_history_hourly(response_df, metadata_df)

    validate_data_quality(candidate_df)
    incoming_rows = validate_ingestions(candidate_df, metadata_df, pending)

    final_df = resolve_overlaps(candidate_df).cache()
    final_rows = final_df.count()

    print(f"Incoming rows trước overlap resolution: {incoming_rows}")
    print(f"Rows sau overlap resolution: {final_rows}")

    merge_silver(spark, final_df)

    # Chỉ mark processed sau khi Silver ghi thành công.
    mark_processed(spark, metadata_df)

    final_df.unpersist()
    validate_silver(spark)

    return {"status": "SUCCESS", "written_rows": final_rows}