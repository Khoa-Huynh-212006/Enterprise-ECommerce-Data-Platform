from pyspark.sql import DataFrame, SparkSession
from delta.tables import DeltaTable
from pyspark.sql.types import TimestampType
from pyspark.sql import functions as F
    
# Lấy ra danh sách các Paths đã commit (_SUCCESS)
def discover_committed_forecast_ingestions(
    bronze_client,
    forecast_root: str,
) -> list[str]:

    committed_ingestion_paths = []

    paths = bronze_client.get_paths(
        path=forecast_root,
        recursive=True,
    )

    for path in paths:
        if (
            not path.is_directory
            and path.name.endswith("/_SUCCESS")
        ):
            ingestion_path = path.name.removesuffix("/_SUCCESS")

            committed_ingestion_paths.append(
                ingestion_path
            )

    return sorted(committed_ingestion_paths)

# Lấy ra danh sách các ingestion_id đã processed
def get_processed_forecast_ingestion_ids(
    spark: SparkSession,
    silver_path: str,
) -> set[str]:
    if not DeltaTable.isDeltaTable(spark, silver_path): # check xem có phải delta dataset
        return set()
    
    df_silver = spark.read.format("delta").load(silver_path)

    rows = (
        df_silver
        .select("ingestion_id")
        .distinct()
        .collect() # chuyển kết quả Spark về Python Driver
    )

    return {
        row["ingestion_id"]
        for row in rows
        if row["ingestion_id"] is not None
    }


# Lấy ra danh sách các ingestion_id chưa processed
def find_pending_forecast_ingestions(
    committed_ingestion_paths: list[str],
    processed_ingestion_ids: set[str],
) -> list[str]:
    final_ingestion_paths = []

    for ingestion_path in committed_ingestion_paths:
        ingestion_id = ingestion_path.rstrip("/").split("/")[-1].removeprefix("ingestion_id=")
        if ingestion_id not in processed_ingestion_ids:
            final_ingestion_paths.append(ingestion_path)

    return final_ingestion_paths

# Load các ingestion_id chưa processed
def load_pending_forecast_bronze(
    spark: SparkSession,
    pending_ingestion_paths: list[str],
) -> tuple[DataFrame, DataFrame]:
    if not pending_ingestion_paths:
        raise ValueError("Không có pending Forecast ingestion để load.")

    response_paths = [f"{path}/response.json" for path in pending_ingestion_paths]
    metadata_paths = [f"{path}/metadata.json" for path in pending_ingestion_paths]

    df_response = spark.read.format("json").option("multiline", True).load(response_paths) #bulk load
    df_metadata = spark.read.format("json").option("multiline", True).load(metadata_paths)

    return df_response, df_metadata


# Thêm vào ingestion_id từ Path cho response df
def extract_forecast_ingestion_context(
    df_response: DataFrame,
) -> DataFrame:
    df_response = (
        df_response
        .withColumn("_source_file_path", F.col("_metadata.file_path"))
        .withColumn("ingestion_id", F.regexp_extract("_source_file_path", r"ingestion_id=([^/]+)", 1))
    )
    
    return df_response


#Làm phẳng hourly array
def flatten_hourly_arrays(
    df_response: DataFrame,
) -> DataFrame:

    df_response = df_response.withColumn(
        "hourly",
        F.arrays_zip(
            "hourly.time",
            "hourly.temperature_2m",
            "hourly.relative_humidity_2m",
            "hourly.precipitation",
            "hourly.wind_speed_10m",
            "hourly.weather_code",
        )
    )

    df_response = df_response.withColumn(
        "hourly",
        F.explode("hourly")
    )

    df_response = df_response.select(
        "ingestion_id",
        "_source_file_path",
        "latitude",
        "longitude",
        "timezone",
        F.col("hourly.time").alias("time"),
        F.col("hourly.temperature_2m").alias("temperature_2m"),
        F.col("hourly.relative_humidity_2m").alias("relative_humidity_2m"),
        F.col("hourly.precipitation").alias("precipitation"),
        F.col("hourly.wind_speed_10m").alias("wind_speed_10m"),
        F.col("hourly.weather_code").alias("weather_code"),
    )

    return df_response


# Join response và metadata    
def attach_forecast_metadata(
    df_response: DataFrame,
    df_metadata: DataFrame,
) -> DataFrame:

    df = df_response.join(
        df_metadata,
        on="ingestion_id",
        how="left",
    )

    return df

# Lấy columns cần thiết và đổi tên phù hợp
def standardize_forecast_columns(
    df: DataFrame,
) -> DataFrame:

    df = df.select(
        "warehouse_id",
        "ingestion_id",
        "_source_file_path",

        F.col("logical_at").alias("snapshot_at"),
        F.col("requested_at").alias("retrieved_at"),
        F.col("time").alias("forecast_time_local"),

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

    return df

def normalize_forecast_timestamps(
    df: DataFrame,
) -> DataFrame:

    df = (
        df
        .withColumn(
            "retrieved_at",
            F.col("retrieved_at").cast(TimestampType())
        )
        .withColumn(
            "snapshot_at",
            F.col("snapshot_at").cast(TimestampType())
        )
        .withColumn(
            "forecast_time",
            F.to_utc_timestamp(
                F.col("forecast_time_local").cast(TimestampType()),
                "Asia/Ho_Chi_Minh"
            )
        )
        .drop("forecast_time_local")
    )

    return df


def transform_forecast_hourly(
    df_response: DataFrame,
    df_metadata: DataFrame,
) -> DataFrame:

    df = extract_forecast_ingestion_context(df_response)

    df = flatten_hourly_arrays(df)

    df = attach_forecast_metadata(
        df,
        df_metadata,
    )

    df = standardize_forecast_columns(df)

    df = normalize_forecast_timestamps(df)

    return df

def profile_forecast_data_quality(
    df: DataFrame,
) -> dict[str, int]:

    quality_row = (
        df
        .select(
            F.sum(
                F.col("ingestion_id").isNull().cast("int")
            ).alias("ingestion_id_null_count"),

            F.sum(
                F.col("warehouse_id").isNull().cast("int")
            ).alias("warehouse_id_null_count"),

            F.sum(
                F.col("snapshot_at").isNull().cast("int")
            ).alias("snapshot_at_null_count"),

            F.sum(
                F.col("retrieved_at").isNull().cast("int")
            ).alias("retrieved_at_null_count"),

            F.sum(
                F.col("forecast_time").isNull().cast("int")
            ).alias("forecast_time_null_count"),

            F.sum(
                F.col("temperature_2m").isNull().cast("int")
            ).alias("temperature_2m_null_count"),

            F.sum(
                F.col("relative_humidity_2m").isNull().cast("int")
            ).alias("relative_humidity_2m_null_count"),

            F.sum(
                F.col("precipitation").isNull().cast("int")
            ).alias("precipitation_null_count"),

            F.sum(
                F.col("wind_speed_10m").isNull().cast("int")
            ).alias("wind_speed_10m_null_count"),

            F.sum(
                F.col("weather_code").isNull().cast("int")
            ).alias("weather_code_null_count"),

            F.sum(
                F.when(
                    F.col("relative_humidity_2m") < 0,
                    1,
                ).otherwise(0)
            ).alias("relative_humidity_2m_negative_count"),

            F.sum(
                F.when(
                    F.col("relative_humidity_2m") > 100,
                    1,
                ).otherwise(0)
            ).alias("relative_humidity_2m_above_100_count"),

            F.sum(
                F.when(
                    F.col("precipitation") < 0,
                    1,
                ).otherwise(0)
            ).alias("precipitation_negative_count"),

            F.sum(
                F.when(
                    F.col("wind_speed_10m") < 0,
                    1,
                ).otherwise(0)
            ).alias("wind_speed_10m_negative_count"),
        )
        .first()
    )

    duplicate_grain_count = (
        df
        .groupBy(
            "warehouse_id",
            "ingestion_id",
            "forecast_time",
        )
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    quality_result = quality_row.asDict()

    quality_result["duplicate_grain_count"] = (
        duplicate_grain_count
    )

    return quality_result


def assert_forecast_data_quality(
    dq_result: dict[str, int],
) -> None:

    failed_checks = {
        metric: count
        for metric, count in dq_result.items()
        if count != 0
    }

    if failed_checks:
        error_details = ", ".join(
            f"{metric}={count}"
            for metric, count in failed_checks.items()
        )

        raise ValueError(
            f"Forecast Data Quality FAILED: {error_details}"
        )


def extract_ingestion_id_from_path(
    ingestion_path: str,
) -> str:
    return (
        ingestion_path
        .rstrip("/")
        .split("/")[-1]
        .removeprefix("ingestion_id=")
    )

def profile_forecast_validation(
    df: DataFrame,
    pending_ingestion_paths: list[str],
    expected_rows_per_ingestion: int = 48,
) -> dict[str, int]:

    pending_ingestion_ids = {
        extract_ingestion_id_from_path(path)
        for path in pending_ingestion_paths
    }

    pending_ingestion_count = len(
        pending_ingestion_ids
    )

    expected_total_rows = (
        pending_ingestion_count
        * expected_rows_per_ingestion
    )

    actual_total_rows = df.count()

    rows_per_ingestion = (
        df
        .groupBy("ingestion_id")
        .count()
    )

    actual_ingestion_ids = {
        row["ingestion_id"]
        for row in (
            rows_per_ingestion
            .select("ingestion_id")
            .collect()
        )
    }

    missing_ingestion_ids = (
        pending_ingestion_ids
        - actual_ingestion_ids
    )

    unexpected_ingestion_ids = (
        actual_ingestion_ids
        - pending_ingestion_ids
    )

    invalid_row_count_ingestions = (
        rows_per_ingestion
        .filter(
            F.col("count")
            != expected_rows_per_ingestion
        )
        .count()
    )

    return {
        "pending_ingestion_count":
            pending_ingestion_count,

        "actual_ingestion_count":
            len(actual_ingestion_ids),

        "expected_total_rows":
            expected_total_rows,

        "actual_total_rows":
            actual_total_rows,

        "missing_ingestion_count":
            len(missing_ingestion_ids),

        "unexpected_ingestion_count":
            len(unexpected_ingestion_ids),

        "invalid_row_count_ingestions":
            invalid_row_count_ingestions,
    }

def assert_forecast_validation(
    validation_result: dict[str, int],
) -> None:

    failed_checks = {}

    if (
        validation_result["expected_total_rows"]
        != validation_result["actual_total_rows"]
    ):
        failed_checks["total_row_count_mismatch"] = (
            validation_result["actual_total_rows"]
        )

    if validation_result["missing_ingestion_count"] != 0:
        failed_checks["missing_ingestion_count"] = (
            validation_result["missing_ingestion_count"]
        )

    if validation_result["unexpected_ingestion_count"] != 0:
        failed_checks["unexpected_ingestion_count"] = (
            validation_result["unexpected_ingestion_count"]
        )

    if validation_result["invalid_row_count_ingestions"] != 0:
        failed_checks["invalid_row_count_ingestions"] = (
            validation_result["invalid_row_count_ingestions"]
        )

    if failed_checks:
        error_details = ", ".join(
            f"{metric}={value}"
            for metric, value in failed_checks.items()
        )

        raise ValueError(
            f"Forecast Validation FAILED: {error_details}"
        )



def write_forecast_silver(
    df: DataFrame,
    silver_path: str,
) -> None:

    (
        df.write
        .format("delta")
        .mode("append")
        .save(silver_path)
    )