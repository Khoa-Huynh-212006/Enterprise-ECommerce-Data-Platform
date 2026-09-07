from pyspark.sql import DataFrame, SparkSession, functions as F

BRONZE_ROOT = "s3a://bronze/weather/open_meteo/forecast"
SILVER_PATH = "s3a://silver/weather/open_meteo/forecast_hourly"


def delta_table_exists(spark: SparkSession, path: str) -> bool:
    delta_log = spark._jvm.org.apache.hadoop.fs.Path(f"{path}/_delta_log")
    fs = delta_log.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs.exists(delta_log)


def discover_committed_forecast_ingestions(spark: SparkSession, root: str) -> list[str]:
    path = spark._jvm.org.apache.hadoop.fs.Path(root)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    files = fs.listFiles(path, True)

    committed = []
    while files.hasNext():
        file_path = files.next().getPath().toString()
        if file_path.endswith("/_SUCCESS"):
            committed.append(file_path.removesuffix("/_SUCCESS"))

    return sorted(committed)


def get_processed_ingestion_ids(spark: SparkSession, silver_path: str) -> set[str]:
    if not delta_table_exists(spark, silver_path):
        return set()

    rows = (
        spark.read.format("delta").load(silver_path)
        .select("ingestion_id").distinct().collect()
    )

    return {row["ingestion_id"] for row in rows if row["ingestion_id"] is not None}


def extract_ingestion_id(path: str) -> str:
    return path.rstrip("/").split("/")[-1].removeprefix("ingestion_id=")


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


def transform_forecast_hourly(response_df: DataFrame, metadata_df: DataFrame) -> DataFrame:
    response_df = (
        response_df
        .withColumn("_source_file_path", F.col("_metadata.file_path"))
        .withColumn(
            "ingestion_id",
            F.regexp_extract("_source_file_path", r"ingestion_id=([^/]+)", 1)
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
            )
        )
        .withColumn("_hour", F.explode("_hourly"))
        .select(
            "ingestion_id",
            "_source_file_path",
            F.col("latitude").alias("response_latitude"),
            F.col("longitude").alias("response_longitude"),
            F.col("_hour.time").alias("forecast_time_local"),
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
        F.col("logical_at").alias("snapshot_at"),
        F.col("requested_at").alias("retrieved_at"),
        "requested_latitude",
        "requested_longitude",
    )

    return (
        response_df.join(metadata_df, "ingestion_id", "left")
        .withColumn("snapshot_at", F.col("snapshot_at").cast("timestamp"))
        .withColumn("retrieved_at", F.col("retrieved_at").cast("timestamp"))
        .withColumn(
            "forecast_time",
            F.to_utc_timestamp(F.col("forecast_time_local").cast("timestamp"), "Asia/Ho_Chi_Minh")
        )
        .drop("forecast_time_local")
        .select(
            "warehouse_id",
            "ingestion_id",
            "snapshot_at",
            "retrieved_at",
            "forecast_time",
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


def validate_forecast(df: DataFrame, pending_count: int) -> int:
    stats = df.agg(
        F.count("*").alias("rows"),
        F.sum(F.col("warehouse_id").isNull().cast("int")).alias("null_warehouse"),
        F.sum(F.col("ingestion_id").isNull().cast("int")).alias("null_ingestion"),
        F.sum(F.col("forecast_time").isNull().cast("int")).alias("null_time"),
        F.sum((F.col("relative_humidity_2m") < 0).cast("int")).alias("humidity_below_0"),
        F.sum((F.col("relative_humidity_2m") > 100).cast("int")).alias("humidity_above_100"),
        F.sum((F.col("precipitation") < 0).cast("int")).alias("negative_precipitation"),
        F.sum((F.col("wind_speed_10m") < 0).cast("int")).alias("negative_wind"),
    ).first()

    duplicate_count = (
        df.groupBy("warehouse_id", "ingestion_id", "forecast_time")
        .count().filter(F.col("count") > 1).count()
    )

    expected_rows = pending_count * 48
    print(f"Pending ingestions: {pending_count}")
    print(f"Expected rows: {expected_rows}")
    print(f"Actual rows: {stats['rows']}")
    print(f"Duplicate grain: {duplicate_count}")

    failures = {
        "row_count": stats["rows"] != expected_rows,
        "null_warehouse": stats["null_warehouse"] != 0,
        "null_ingestion": stats["null_ingestion"] != 0,
        "null_time": stats["null_time"] != 0,
        "humidity_below_0": stats["humidity_below_0"] != 0,
        "humidity_above_100": stats["humidity_above_100"] != 0,
        "negative_precipitation": stats["negative_precipitation"] != 0,
        "negative_wind": stats["negative_wind"] != 0,
        "duplicate_grain": duplicate_count != 0,
    }

    failed = [name for name, is_failed in failures.items() if is_failed]
    if failed:
        raise ValueError(f"Forecast Silver validation thất bại: {failed}")

    return stats["rows"]


def run_forecast_silver(spark: SparkSession) -> dict:
    committed = discover_committed_forecast_ingestions(spark, BRONZE_ROOT)
    processed = get_processed_ingestion_ids(spark, SILVER_PATH)
    pending = find_pending_ingestions(committed, processed)

    print(f"Committed ingestions: {len(committed)}")
    print(f"Processed ingestions: {len(processed)}")
    print(f"Pending ingestions: {len(pending)}")

    if not pending:
        print("Không có Forecast ingestion mới, bỏ qua ghi Silver.")
        return {"status": "NO_OP", "written_rows": 0}

    response_df, metadata_df = load_pending_bronze(spark, pending)
    silver_df = transform_forecast_hourly(response_df, metadata_df)
    written_rows = validate_forecast(silver_df, len(pending))

    silver_df.write.format("delta").mode("append").save(SILVER_PATH)

    return {"status": "SUCCESS", "written_rows": written_rows}