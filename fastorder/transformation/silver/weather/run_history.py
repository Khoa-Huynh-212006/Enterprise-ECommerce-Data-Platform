from fastorder.transformation.common.spark_session import create_spark_session
from fastorder.transformation.silver.weather.history_hourly import run_history_silver


def main() -> None:
    spark = create_spark_session("fastorder-weather-history-silver")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    result = run_history_silver(spark)
    print(f"Kết quả: {result}")

    spark.stop()
    print("FASTORDER WEATHER HISTORICAL SILVER: PASS")


if __name__ == "__main__":
    main()