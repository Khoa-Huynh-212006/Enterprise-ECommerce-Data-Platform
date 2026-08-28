from pyspark.sql import DataFrame
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

