from fastorder.transformation.common.spark_session import create_spark_session
from fastorder.transformation.silver.weather.forecast_hourly import run_forecast_silver


def main() -> None:
    spark = create_spark_session("fastorder-weather-forecast-silver")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    result = run_forecast_silver(spark)
    print(f"Kết quả: {result}")

    spark.stop()
    print("FASTORDER WEATHER FORECAST SILVER: PASS")


if __name__ == "__main__":
    main()