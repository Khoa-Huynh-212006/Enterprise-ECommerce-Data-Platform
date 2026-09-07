import argparse

from fastorder.loading.dwh.weather import WEATHER_TABLES, load_weather_table
from fastorder.transformation.common.spark_session import create_spark_session


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--table",
        choices=["forecast", "historical", "all"],
        required=True,
    )
    args = parser.parse_args()

    tables = list(WEATHER_TABLES) if args.table == "all" else [args.table]

    spark = create_spark_session("fastorder-weather-dwh-load")

    for table in tables:
        load_weather_table(spark, table)

    spark.stop()

    print(f"FASTORDER WEATHER → DWH: {len(tables)}/{len(tables)} PASS")


if __name__ == "__main__":
    main()