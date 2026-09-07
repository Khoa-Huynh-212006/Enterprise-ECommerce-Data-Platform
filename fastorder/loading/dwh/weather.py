from pyspark.sql import SparkSession, functions as F

from fastorder.loading.dwh.operational import get_jdbc_config


WEATHER_TABLES = {
    "forecast": {
        "silver_path": "s3a://silver/weather/open_meteo/forecast_hourly",
        "staging_table": "staging.weather_forecast_hourly",
        "grain": ["warehouse_id", "ingestion_id", "forecast_time"],
    },
    "historical": {
        "silver_path": "s3a://silver/weather/open_meteo/historical_forecast_hourly",
        "staging_table": "staging.weather_historical_forecast_hourly",
        "grain": ["warehouse_id", "weather_time"],
    },
}


def validate_staging(df, expected_count: int, grain: list[str]) -> None:
    actual_count = df.count()

    duplicate_count = (
        df.groupBy(*grain)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    null_condition = None
    for column in grain:
        condition = F.col(column).isNull()
        null_condition = condition if null_condition is None else null_condition | condition

    null_grain_count = df.filter(null_condition).count()

    print(f"Expected rows: {expected_count}")
    print(f"Staging rows: {actual_count}")
    print(f"NULL grain: {null_grain_count}")
    print(f"Duplicate grain: {duplicate_count}")

    if actual_count != expected_count:
        raise ValueError("Weather staging row count không khớp Silver")

    if null_grain_count != 0 or duplicate_count != 0:
        raise ValueError("Weather staging vi phạm business grain")


def load_weather_table(spark: SparkSession, name: str) -> None:
    config = WEATHER_TABLES[name]
    url, properties = get_jdbc_config()

    print(f"\nĐang load Weather: {name}")

    silver_df = spark.read.format("delta").load(config["silver_path"])
    silver_count = silver_df.count()

    print(f"Silver rows: {silver_count}")

    (
        silver_df.coalesce(2)
        .write.jdbc(
            url=url,
            table=config["staging_table"],
            mode="overwrite",
            properties=properties,
        )
    )

    staging_df = spark.read.jdbc(
        url=url,
        table=config["staging_table"],
        properties=properties,
    )

    validate_staging(
        df=staging_df,
        expected_count=silver_count,
        grain=config["grain"],
    )

    print(f"FASTORDER WEATHER STAGING {name.upper()}: PASS")