from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable


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
    allow_duplicate_grain: bool = False,
) -> None:

    failed_checks = {
        metric: count
        for metric, count in dq_result.items()
        if count != 0
    }

    if allow_duplicate_grain:
        failed_checks.pop(
            "duplicate_grain_count",
            None,
        )

    if failed_checks:
        raise ValueError(
            "Historical Forecast Data Quality FAILED: "
            f"{failed_checks}"
        )

def build_history_actual_validation_summary(
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

def build_history_expected_validation_summary(
    df_metadata: DataFrame,
) -> DataFrame:

    return (
        df_metadata
        .select(
            "ingestion_id",

            F.col("request_params.start_date")
                .cast("date")
                .alias("window_start"),

            F.col("request_params.end_date")
                .cast("date")
                .alias("window_end"),
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
    )

def profile_history_validation(
    df: DataFrame,
    df_metadata: DataFrame,
    pending_ingestion_paths: list[str],
) -> dict[str, int]:

    expected_ingestion_ids = {
        extract_history_ingestion_id_from_path(path)
        for path in pending_ingestion_paths
    }

    expected_summary = (
        build_history_expected_validation_summary(
            df_metadata
        )
    )

    actual_summary = (
        build_history_ingestion_validation_summary(
            df
        )
    )

    metadata_ingestion_ids = {
        row["ingestion_id"]
        for row in (
            expected_summary
            .select("ingestion_id")
            .distinct()
            .collect()
        )
    }

    actual_ingestion_ids = {
        row["ingestion_id"]
        for row in (
            actual_summary
            .select("ingestion_id")
            .distinct()
            .collect()
        )
    }

    missing_metadata_ids = (
        expected_ingestion_ids
        - metadata_ingestion_ids
    )

    missing_ingestion_ids = (
        expected_ingestion_ids
        - actual_ingestion_ids
    )

    unexpected_ingestion_ids = (
        actual_ingestion_ids
        - expected_ingestion_ids
    )

    expected_total_rows = (
        expected_summary
        .agg(
            F.sum("expected_row_count")
                .alias("expected_total_rows")
        )
        .first()["expected_total_rows"]
        or 0
    )

    actual_total_rows = df.count()

    validation_comparison = (
        actual_summary
        .select(
            "ingestion_id",
            "actual_row_count",
            "distinct_weather_hour_count",
            "actual_start_time",
            "actual_end_time",
            "expected_start_time",
            "expected_end_time",
        )
        .join(
            expected_summary.select(
                "ingestion_id",
                "expected_row_count",
            ),
            on="ingestion_id",
            how="left",
        )
    )

    invalid_row_count_ingestions = (
        validation_comparison
        .filter(
            F.col("actual_row_count")
            != F.col("expected_row_count")
        )
        .count()
    )

    invalid_distinct_hour_ingestions = (
        validation_comparison
        .filter(
            F.col("distinct_weather_hour_count")
            != F.col("expected_row_count")
        )
        .count()
    )

    invalid_time_range_ingestions = (
        validation_comparison
        .filter(
            (
                F.col("actual_start_time")
                != F.col("expected_start_time")
            )
            |
            (
                F.col("actual_end_time")
                != F.col("expected_end_time")
            )
        )
        .count()
    )

    return {
        "pending_ingestion_count":
            len(expected_ingestion_ids),

        "metadata_ingestion_count":
            len(metadata_ingestion_ids),

        "actual_ingestion_count":
            len(actual_ingestion_ids),

        "expected_total_rows":
            int(expected_total_rows),

        "actual_total_rows":
            actual_total_rows,

        "missing_metadata_count":
            len(missing_metadata_ids),

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
        "missing_metadata_count",
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


def resolve_history_overlaps(
    df: DataFrame,
) -> DataFrame:

    weather_fields = [
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "wind_speed_10m",
        "weather_code",
    ]

    conflicting_overlap_count = (
        df
        .groupBy(
            "warehouse_id",
            "weather_time",
        )
        .agg(
            F.countDistinct(
                F.struct(
                    *[
                        F.col(column)
                        for column in weather_fields
                    ]
                )
            ).alias("weather_version_count")
        )
        .filter(
            F.col("weather_version_count") > 1
        )
        .count()
    )

    if conflicting_overlap_count > 0:
        raise ValueError(
            "Historical overlap reconciliation FAILED: "
            f"{conflicting_overlap_count} business keys "
            "contain conflicting weather values."
        )

    window_spec = (
        Window
        .partitionBy(
            "warehouse_id",
            "weather_time",
        )
        .orderBy(
            F.col("retrieved_at").desc(),
            F.col("ingestion_id").desc(),
        )
    )

    return (
        df
        .withColumn(
            "_overlap_rank",
            F.row_number().over(window_spec),
        )
        .filter(
            F.col("_overlap_rank") == 1
        )
        .drop(
            "_overlap_rank"
        )
    )


def prepare_history_silver_output(
    df: DataFrame,
) -> DataFrame:

    return (
        df
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

def assert_no_conflicting_history_with_silver(
    spark: SparkSession,
    df_incoming: DataFrame,
    silver_path: str,
) -> None:

    if not DeltaTable.isDeltaTable(
        spark,
        silver_path,
    ):
        return

    df_existing = (
        spark.read
        .format("delta")
        .load(silver_path)
    )

    df_matches = (
        df_incoming.alias("incoming")
        .join(
            df_existing.alias("existing"),
            on=[
                "warehouse_id",
                "weather_time",
            ],
            how="inner",
        )
    )

    conflicting_count = (
        df_matches
        .filter(
            ~F.col(
                "incoming.temperature_2m"
            ).eqNullSafe(
                F.col("existing.temperature_2m")
            )
            |
            ~F.col(
                "incoming.relative_humidity_2m"
            ).eqNullSafe(
                F.col(
                    "existing.relative_humidity_2m"
                )
            )
            |
            ~F.col(
                "incoming.precipitation"
            ).eqNullSafe(
                F.col("existing.precipitation")
            )
            |
            ~F.col(
                "incoming.wind_speed_10m"
            ).eqNullSafe(
                F.col("existing.wind_speed_10m")
            )
            |
            ~F.col(
                "incoming.weather_code"
            ).eqNullSafe(
                F.col("existing.weather_code")
            )
        )
        .count()
    )

    if conflicting_count > 0:
        raise ValueError(
            "Historical Silver conflict FAILED: "
            f"{conflicting_count} business keys "
            "contain different weather values "
            "between incoming data and existing Silver."
        )

def merge_history_silver(
    spark: SparkSession,
    df: DataFrame,
    silver_path: str,
) -> None:

    if not DeltaTable.isDeltaTable(
        spark,
        silver_path,
    ):
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .save(silver_path)
        )

        return

    target = (
        DeltaTable.forPath(
            spark,
            silver_path,
        )
    )

    (
        target.alias("target")
        .merge(
            df.alias("source"),
            """
            target.warehouse_id = source.warehouse_id
            AND
            target.weather_time = source.weather_time
            """,
        )
        .whenMatchedUpdateAll(
            condition=(
                "source.retrieved_at "
                "> target.retrieved_at"
            )
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

def build_history_processed_ingestions(
    df_metadata: DataFrame,
) -> DataFrame:

    return (
        df_metadata
        .select(
            "ingestion_id",
            "warehouse_id",

            F.col("request_params.start_date")
                .cast("date")
                .alias("window_start"),

            F.col("request_params.end_date")
                .cast("date")
                .alias("window_end"),

            F.col("requested_at")
                .cast("timestamp")
                .alias("retrieved_at"),
        )
        .dropDuplicates(
            ["ingestion_id"]
        )
        .withColumn(
            "silver_processed_at",
            F.current_timestamp(),
        )
    )    



def mark_history_ingestions_processed(
    spark: SparkSession,
    df_processed_ingestions: DataFrame,
    control_path: str,
) -> None:

    if not DeltaTable.isDeltaTable(
        spark,
        control_path,
    ):
        (
            df_processed_ingestions.write
            .format("delta")
            .mode("overwrite")
            .save(control_path)
        )

        return

    target = (
        DeltaTable.forPath(
            spark,
            control_path,
        )
    )

    (
        target.alias("target")
        .merge(
            df_processed_ingestions.alias("source"),
            (
                "target.ingestion_id "
                "= source.ingestion_id"
            ),
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

def get_processed_history_ingestion_ids(
    spark: SparkSession,
    control_path: str,
) -> set[str]:

    if not DeltaTable.isDeltaTable(
        spark,
        control_path,
    ):
        return set()

    rows = (
        spark.read
        .format("delta")
        .load(control_path)
        .select("ingestion_id")
        .distinct()
        .collect()
    )

    return {
        row["ingestion_id"]
        for row in rows
    }


def discover_committed_history_ingestions(
    bronze_client,
    history_root: str,
) -> list[str]:

    committed_ingestion_paths = []

    paths = bronze_client.get_paths(
        path=history_root,
        recursive=True,
    )

    for path in paths:

        if (
            not path.is_directory
            and path.name.endswith("/_SUCCESS")
        ):
            ingestion_path = (
                path.name.removesuffix("/_SUCCESS")
            )

            committed_ingestion_paths.append(
                ingestion_path
            )

    return sorted(
        committed_ingestion_paths
    )


def find_pending_history_ingestions(
    committed_ingestion_paths: list[str],
    processed_ingestion_ids: set[str],
) -> list[str]:

    pending_ingestion_paths = []

    for ingestion_path in committed_ingestion_paths:

        ingestion_id = (
            extract_history_ingestion_id_from_path(
                ingestion_path
            )
        )

        if ingestion_id not in processed_ingestion_ids:
            pending_ingestion_paths.append(
                ingestion_path
            )

    return sorted(
        pending_ingestion_paths
    )


def get_processed_history_ingestion_ids(
    spark: SparkSession,
    control_path: str,
) -> set[str]:

    if not DeltaTable.isDeltaTable(
        spark,
        control_path,
    ):
        return set()

    rows = (
        spark.read
        .format("delta")
        .load(control_path)
        .select("ingestion_id")
        .distinct()
        .collect()
    )

    return {
        row["ingestion_id"]
        for row in rows
    }