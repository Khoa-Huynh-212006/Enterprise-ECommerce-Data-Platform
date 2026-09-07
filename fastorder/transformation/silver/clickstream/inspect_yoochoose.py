from fastorder.transformation.common.spark_session import create_spark_session


spark = create_spark_session("fastorder-inspect-yoochoose")

df = spark.read.parquet("s3a://bronze/clickstream/yoochoose")

df.printSchema()

print(f"Rows: {df.count()}")
print(f"Files: {len(df.inputFiles())}")

df.select(
    "session_id",
    "event_timestamp",
    "item_id",
    "category",
    "_source_file_path",
    "_source_file_etag",
    "_ingestion_id",
    "_ingested_at"
).show(10, truncate=False)

spark.stop()