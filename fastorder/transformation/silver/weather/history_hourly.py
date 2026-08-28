from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F




def extract_history_ingestion_context(
    df_response: DataFrame,
) -> DataFrame:

    return (
        df_response
        .withColumn(
            "_source_file_path",
            F.col("_metadata.file_path"),
        )
        .withColumn(
            "ingestion_id",
            F.regexp_extract(
                F.col("_source_file_path"),
                r"ingestion_id=([^/]+)",
                1,
            ),
        )
    )

def flatten_history_hourly_arrays(
    df_response: DataFrame,
) -> DataFrame:

    df_zipped = (
        df_response
        .withColumn(
            "_hourly",
            F.arrays_zip(
                F.col("hourly.time"),
                F.col("hourly.temperature_2m"),
                F.col("hourly.relative_humidity_2m"),
                F.col("hourly.precipitation"),
                F.col("hourly.wind_speed_10m"),
                F.col("hourly.weather_code"),
            ),
        )
    )

    df_exploded = (
        df_zipped
        .withColumn(
            "_hour",
            F.explode(F.col("_hourly")),
        )
    )

    return (
        df_exploded
        .select(
            "ingestion_id",
            "_source_file_path",

            F.col("latitude")
                .alias("response_latitude"),

            F.col("longitude")
                .alias("response_longitude"),

            "timezone",

            F.col("_hour.time")
                .alias("time"),

            F.col("_hour.temperature_2m")
                .alias("temperature_2m"),

            F.col("_hour.relative_humidity_2m")
                .alias("relative_humidity_2m"),

            F.col("_hour.precipitation")
                .alias("precipitation"),

            F.col("_hour.wind_speed_10m")
                .alias("wind_speed_10m"),

            F.col("_hour.weather_code")
                .alias("weather_code"),
        )
    )


def attach_history_metadata(
    df_response: DataFrame,
    df_metadata: DataFrame,
) -> DataFrame:

    df_metadata_selected = (
        df_metadata
        .select(
            "ingestion_id",
            "warehouse_id",
            "requested_at",
            "requested_latitude",
            "requested_longitude",

            F.col("request_params.start_date")
                .alias("window_start"),

            F.col("request_params.end_date")
                .alias("window_end"),
        )
    )

    return (
        df_response
        .join(
            df_metadata_selected,
            on="ingestion_id",
            how="left",
        )
    )


def standardize_history_columns(
    df: DataFrame,
) -> DataFrame:

    return (
        df
        .select(
            "warehouse_id",
            "ingestion_id",
            "_source_file_path",

            F.col("requested_at")
                .alias("retrieved_at"),

            "window_start",
            "window_end",

            F.col("time")
                .alias("weather_time_local"),

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

def normalize_history_time(
    df: DataFrame,
) -> DataFrame:

    return (
        df
        .withColumn(
            "retrieved_at",
            F.col("retrieved_at").cast("timestamp"),
        )
        .withColumn(
            "window_start",
            F.to_date(F.col("window_start")),
        )
        .withColumn(
            "window_end",
            F.to_date(F.col("window_end")),
        )
        .withColumn(
            "weather_time",
            F.to_utc_timestamp(
                F.col("weather_time_local")
                    .cast("timestamp"),
                "Asia/Ho_Chi_Minh",
            ),
        )
        .drop(
            "weather_time_local"
        )
    )


def transform_history_hourly(
    df_response: DataFrame,
    df_metadata: DataFrame,
) -> DataFrame:

    df_context = (
        extract_history_ingestion_context(
            df_response
        )
    )

    df_flattened = (
        flatten_history_hourly_arrays(
            df_context
        )
    )

    df_enriched = (
        attach_history_metadata(
            df_response=df_flattened,
            df_metadata=df_metadata,
        )
    )

    df_standardized = (
        standardize_history_columns(
            df_enriched
        )
    )

    return (
        normalize_history_time(
            df_standardized
        )
    )

def profile_history_data_quality(
    df: DataFrame,
) -> dict[str, int]:

    dq_row = (
        df
        .agg(
            F.sum(
                F.col("warehouse_id").isNull().cast("int")
            ).alias("null_warehouse_id"),

            F.sum(
                F.col("ingestion_id").isNull().cast("int")
            ).alias("null_ingestion_id"),

            F.sum(
                F.col("weather_time").isNull().cast("int")
            ).alias("null_weather_time"),

            F.sum(
                F.col("retrieved_at").isNull().cast("int")
            ).alias("null_retrieved_at"),

            F.sum(
                F.col("window_start").isNull().cast("int")
            ).alias("null_window_start"),

            F.sum(
                F.col("window_end").isNull().cast("int")
            ).alias("null_window_end"),

            F.sum(
                F.col("temperature_2m").isNull().cast("int")
            ).alias("null_temperature_2m"),

            F.sum(
                F.col("relative_humidity_2m").isNull().cast("int")
            ).alias("null_relative_humidity_2m"),

            F.sum(
                F.col("precipitation").isNull().cast("int")
            ).alias("null_precipitation"),

            F.sum(
                F.col("wind_speed_10m").isNull().cast("int")
            ).alias("null_wind_speed_10m"),

            F.sum(
                F.col("weather_code").isNull().cast("int")
            ).alias("null_weather_code"),

            F.sum(
                (
                    F.col("relative_humidity_2m") < 0
                ).cast("int")
            ).alias("humidity_below_0"),

            F.sum(
                (
                    F.col("relative_humidity_2m") > 100
                ).cast("int")
            ).alias("humidity_above_100"),

            F.sum(
                (
                    F.col("precipitation") < 0
                ).cast("int")
            ).alias("negative_precipitation"),

            F.sum(
                (
                    F.col("wind_speed_10m") < 0
                ).cast("int")
            ).alias("negative_wind_speed"),

            F.sum(
                (
                    F.col("window_start")
                    > F.col("window_end")
                ).cast("int")
            ).alias("invalid_window_order"),
        )
        .first()
        .asDict()
    )

    duplicate_grain_count = (
        df
        .groupBy(
            "warehouse_id",
            "weather_time",
        )
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    dq_row["duplicate_grain_count"] = (
        duplicate_grain_count
    )

    return dq_row



def assert_history_data_quality(
    dq_result: dict[str, int],
) -> None:

    failed_checks = {
        metric: count
        for metric, count in dq_result.items()
        if count != 0
    }

    if failed_checks:
        raise ValueError(
            "Historical Forecast Data Quality FAILED: "
            f"{failed_checks}"
        )

def build_history_ingestion_validation_summary(
    df: DataFrame,
) -> DataFrame:

    summary = (
        df
        .groupBy(
            "ingestion_id",
            "warehouse_id",
            "window_start",
            "window_end",
        )
        .agg(
            F.count("*")
                .alias("actual_row_count"),

            F.countDistinct("weather_time")
                .alias("distinct_weather_hour_count"),

            F.min("weather_time")
                .alias("actual_start_time"),

            F.max("weather_time")
                .alias("actual_end_time"),
        )
        .withColumn(
            "expected_day_count",
            F.datediff(
                F.col("window_end"),
                F.col("window_start"),
            ) + F.lit(1),
        )
        .withColumn(
            "expected_row_count",
            F.col("expected_day_count") * F.lit(24),
        )
        .withColumn(
            "expected_start_time",
            F.to_utc_timestamp(
                F.to_timestamp(
                    F.concat(
                        F.col("window_start").cast("string"),
                        F.lit(" 00:00:00"),
                    )
                ),
                "Asia/Ho_Chi_Minh",
            ),
        )
        .withColumn(
            "expected_end_time",
            F.to_utc_timestamp(
                F.to_timestamp(
                    F.concat(
                        F.col("window_end").cast("string"),
                        F.lit(" 23:00:00"),
                    )
                ),
                "Asia/Ho_Chi_Minh",
            ),
        )
    )

    return summary



def extract_history_ingestion_id_from_path(
    ingestion_path: str,
) -> str:

    return (
        ingestion_path
        .rstrip("/")
        .split("/")[-1]
        .removeprefix("ingestion_id=")
    )

def profile_history_validation(
    df: DataFrame,
    pending_ingestion_paths: list[str],
) -> dict[str, int]:

    expected_ingestion_ids = {
        extract_history_ingestion_id_from_path(path)
        for path in pending_ingestion_paths
    }

    summary = (
        build_history_ingestion_validation_summary(
            df
        )
    )

    actual_ingestion_ids = {
        row["ingestion_id"]
        for row in (
            summary
            .select("ingestion_id")
            .collect()
        )
    }

    expected_total_rows = (
        summary
        .agg(
            F.sum("expected_row_count")
                .alias("expected_total_rows")
        )
        .first()["expected_total_rows"]
        or 0
    )

    actual_total_rows = df.count()

    invalid_row_count_ingestions = (
        summary
        .filter(
            F.col("actual_row_count")
            != F.col("expected_row_count")
        )
        .count()
    )

    invalid_distinct_hour_ingestions = (
        summary
        .filter(
            F.col("distinct_weather_hour_count")
            != F.col("expected_row_count")
        )
        .count()
    )

    invalid_time_range_ingestions = (
        summary
        .filter(
            (F.col("actual_start_time")
             != F.col("expected_start_time"))
            |
            (F.col("actual_end_time")
             != F.col("expected_end_time"))
        )
        .count()
    )

    missing_ingestion_ids = (
        expected_ingestion_ids
        - actual_ingestion_ids
    )

    unexpected_ingestion_ids = (
        actual_ingestion_ids
        - expected_ingestion_ids
    )

    return {
        "pending_ingestion_count":
            len(expected_ingestion_ids),

        "actual_ingestion_count":
            len(actual_ingestion_ids),

        "expected_total_rows":
            int(expected_total_rows),

        "actual_total_rows":
            actual_total_rows,

        "missing_ingestion_count":
            len(missing_ingestion_ids),

        "unexpected_ingestion_count":
            len(unexpected_ingestion_ids),

        "invalid_row_count_ingestions":
            invalid_row_count_ingestions,

        "invalid_distinct_hour_ingestions":
            invalid_distinct_hour_ingestions,

        "invalid_time_range_ingestions":
            invalid_time_range_ingestions,
    }
    
def assert_history_validation(
    validation_result: dict[str, int],
) -> None:

    failed_checks = {}

    if (
        validation_result["expected_total_rows"]
        != validation_result["actual_total_rows"]
    ):
        failed_checks["total_row_count_mismatch"] = {
            "expected":
                validation_result["expected_total_rows"],
            "actual":
                validation_result["actual_total_rows"],
        }

    for metric in [
        "missing_ingestion_count",
        "unexpected_ingestion_count",
        "invalid_row_count_ingestions",
        "invalid_distinct_hour_ingestions",
        "invalid_time_range_ingestions",
    ]:

        if validation_result[metric] != 0:
            failed_checks[metric] = (
                validation_result[metric]
            )

    if failed_checks:
        raise ValueError(
            "Historical Forecast Validation FAILED: "
            f"{failed_checks}"
        )

def load_pending_history_bronze(
    spark: SparkSession,
    pending_ingestion_paths: list[str],
    bronze_abfss_root: str,
) -> tuple[DataFrame, DataFrame]:

    if not pending_ingestion_paths:
        raise ValueError(
            "Không có pending Historical ingestion để load."
        )

    response_paths = [
        (
            f"{bronze_abfss_root}/"
            f"{path.lstrip('/')}/response.json"
        )
        for path in pending_ingestion_paths
    ]

    metadata_paths = [
        (
            f"{bronze_abfss_root}/"
            f"{path.lstrip('/')}/metadata.json"
        )
        for path in pending_ingestion_paths
    ]

    df_response = (
        spark.read
        .format("json")
        .option("multiline", True)
        .load(response_paths)
    )

    df_metadata = (
        spark.read
        .format("json")
        .option("multiline", True)
        .load(metadata_paths)
    )

    return df_response, df_metadata