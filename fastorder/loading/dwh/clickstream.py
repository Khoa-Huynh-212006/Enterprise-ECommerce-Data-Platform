import time

from fastorder.loading.dwh.operational import get_jdbc_config
from fastorder.transformation.common.spark_session import create_spark_session


SILVER_PATH = "s3a://silver/clickstream/yoochoose"
STAGING_TABLE = "staging.yoochoose_clicks"


def main() -> None:
    spark = create_spark_session("fastorder-yoochoose-dwh-load")
    url, properties = get_jdbc_config()

    df = spark.read.format("delta").load(SILVER_PATH)
    silver_count = df.count()
    input_partitions = df.rdd.getNumPartitions()

    print(f"Silver events: {silver_count}")
    print(f"Input partitions: {input_partitions}")
    print("Bắt đầu full refresh PostgreSQL staging...")

    started_at = time.time()

    # V1: full refresh để có baseline về chi phí JDBC + PostgreSQL.
    (
        df.coalesce(4)
        .write.jdbc(
            url=url,
            table=STAGING_TABLE,
            mode="overwrite",
            properties=properties,
        )
    )

    elapsed = time.time() - started_at

    # Chỉ query COUNT(*) từ PostgreSQL, không đọc lại 33 triệu dòng vào Spark.
    count_query = f"(SELECT COUNT(*) AS row_count FROM {STAGING_TABLE}) q"
    staging_count = spark.read.jdbc(
        url=url,
        table=count_query,
        properties=properties,
    ).first()["row_count"]

    print(f"Staging events: {staging_count}")
    print(f"Load time: {elapsed:.2f} seconds")

    if staging_count != silver_count:
        raise ValueError(
            f"Row count mismatch: Silver={silver_count}, Staging={staging_count}"
        )

    spark.stop()
    print("FASTORDER YOOCHOOSE → POSTGRES DWH: PASS")


if __name__ == "__main__":
    main()