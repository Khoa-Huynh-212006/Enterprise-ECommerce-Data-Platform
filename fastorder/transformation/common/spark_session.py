import os

from pyspark.sql import SparkSession


def create_spark_session(
    app_name: str,
) -> SparkSession:

    minio_endpoint = os.environ["MINIO_ENDPOINT"]
    minio_access_key = os.environ["MINIO_ROOT_USER"]
    minio_secret_key = os.environ["MINIO_ROOT_PASSWORD"]

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            minio_endpoint,
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            minio_access_key,
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            minio_secret_key,
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true",
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false",
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

    return spark