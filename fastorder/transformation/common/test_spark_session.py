from fastorder.transformation.common.spark_session import (
    create_spark_session,
)


spark = create_spark_session(
    app_name="fastorder-spark-session-test",
)


print(
    "Spark version:",
    spark.version,
)

print(
    "Spark master:",
    spark.sparkContext.master,
)


df = spark.read.parquet(
    "s3a://bronze/orders"
)


print(
    "Orders count:",
    df.count(),
)


spark.stop()


print(
    "FASTORDER COMMON SPARK SESSION: PASS"
)