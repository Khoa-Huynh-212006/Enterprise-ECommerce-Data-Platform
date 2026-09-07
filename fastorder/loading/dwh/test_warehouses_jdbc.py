import os

from fastorder.transformation.common.spark_session import create_spark_session


SILVER_PATH = "s3a://silver/operational/warehouses"
DWH_TABLE = "staging.warehouses"


def main() -> None:
    spark = create_spark_session("fastorder-dwh-warehouses-test")

    jdbc_url = (
        f"jdbc:postgresql://{os.environ['DWH_DB_HOST']}:"
        f"{os.environ['DWH_DB_PORT']}/{os.environ['DWH_DB_NAME']}"
    )

    properties = {
        "user": os.environ["DWH_DB_USER"],
        "password": os.environ["DWH_DB_PASSWORD"],
        "driver": "org.postgresql.Driver",
    }

    df = spark.read.format("delta").load(SILVER_PATH)
    silver_count = df.count()

    print(f"Silver warehouses: {silver_count}")

    df.write.jdbc(
        url=jdbc_url,
        table=DWH_TABLE,
        mode="overwrite",
        properties=properties,
    )

    dwh_df = spark.read.jdbc(
        url=jdbc_url,
        table=DWH_TABLE,
        properties=properties,
    )

    dwh_count = dwh_df.count()

    print(f"DWH warehouses: {dwh_count}")

    if silver_count != dwh_count:
        raise ValueError(
            f"Row count mismatch: Silver={silver_count}, DWH={dwh_count}"
        )

    spark.stop()
    print("FASTORDER SILVER → POSTGRES DWH JDBC: PASS")


if __name__ == "__main__":
    main()