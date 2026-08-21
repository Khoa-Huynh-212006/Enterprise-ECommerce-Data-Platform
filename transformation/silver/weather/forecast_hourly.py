from pyspark.sql import DataFrame, SparkSession
from delta.tables import DeltaTable


# Load df_metadata và df_response từ Ingestion_Path
def load_forecast_bronze(
    spark: SparkSession, 
    ingestion_path: str
)-> tuple[DataFrame, DataFrame]:
    if not path_exists(f"{ingestion_path}/_SUCCESS"):
        raise ValueError(f"Bronze ingestion path này chưa commit: {ingestion_path}")
    
    df_metadata = spark.read.format("json").option("multiline", True).load(f"{ingestion_path}/metadata.json")
    df_response = spark.read.format("json").option("multiline", True).load(f"{ingestion_path}/response.json")
    return df_response, df_metadata
    
# Lấy ra danh sách các Paths đã commit (_SUCCESS)
def discover_committed_forecast_ingestions(
    bronze_client,
    forecast_root: str,
) -> list[str]:

    committed_ingestion_paths = []

    paths = bronze_client.get_paths(
        path=forecast_root,
        recursive=True,
    )

    for path in paths:
        if (
            not path.is_directory
            and path.name.endswith("/_SUCCESS")
        ):
            ingestion_path = path.name.removesuffix("/_SUCCESS")

            committed_ingestion_paths.append(
                ingestion_path
            )

    return sorted(committed_ingestion_paths)

# Lấy ra danh sách các ingestion_id đã processed
def get_processed_forecast_ingestion_ids(
    spark: SparkSession,
    silver_path: str,
) -> set[str]:
    if not DeltaTable.isDeltaTable(spark, silver_path): # check xem có phải delta dataset
        return set()
    
    df_silver = spark.read.format("delta").load(silver_path)

    rows = (
        df_silver
        .select("ingestion_id")
        .distinct()
        .collect() # chuyển kết quả Spark về Python Driver
    )

    return {
        row["ingestion_id"]
        for row in rows
        if row["ingestion_id"] is not None
    }


# Lấy ra danh sách các ingestion_id chưa processed
def find_pending_forecast_ingestions(
    committed_ingestion_paths: list[str],
    processed_ingestion_ids: set[str],
) -> list[str]:
    final_ingestion_paths = []

    for ingestion_path in committed_ingestion_paths:
        ingestion_id = ingestion_path.rstrip("/").split("/")[-1].removeprefix("ingestion_id=")
        if ingestion_id not in processed_ingestion_ids:
            final_ingestion_paths.append(ingestion_path)

    return final_ingestion_paths

# Load các ingestion_id chưa processed
def load_pending_forecast_bronze(
    spark: SparkSession,
    pending_ingestion_paths: list[str],
) -> tuple[DataFrame, DataFrame]:
    if not pending_ingestion_paths:
        raise ValueError("Không có pending Forecast ingestion để load.")

    response_paths = [f"{path}/response.json" for path in pending_ingestion_paths]
    metadata_paths = [f"{path}/metadata.json" for path in pending_ingestion_paths]

    df_response = spark.read.format("json").option("multiline", True).load(response_paths) #bulk load
    df_metadata = spark.read.format("json").option("multiline", True).load(metadata_paths)

    return df_response, df_metadata


# Thêm vào ingestion_id từ Path cho response df
def extract_forecast_ingestion_context(
    df_response: DataFrame,
) -> DataFrame:
    df_response = (
        df_response
        .withColumn("_source_file_path", F.col("_metadata.file_path"))
        .withColumn("ingestion_id", F.regexp_extract("_source_file_path", r"ingestion_id=([^/]+)", 1))
    )
    
    return df_response


#Làm phẳng hourly array
def flatten_hourly_arrays(
    df_response: DataFrame,
) -> DataFrame:

    df_response = df_response.withColumn(
        "hourly",
        F.arrays_zip(
            "hourly.time",
            "hourly.temperature_2m",
            "hourly.relative_humidity_2m",
            "hourly.precipitation",
            "hourly.wind_speed_10m",
            "hourly.weather_code",
        )
    )

    df_response = df_response.withColumn(
        "hourly",
        F.explode("hourly")
    )

    df_response = df_response.select(
        "ingestion_id",
        "_source_file_path",
        "latitude",
        "longitude",
        "timezone",
        F.col("hourly.time").alias("time"),
        F.col("hourly.temperature_2m").alias("temperature_2m"),
        F.col("hourly.relative_humidity_2m").alias("relative_humidity_2m"),
        F.col("hourly.precipitation").alias("precipitation"),
        F.col("hourly.wind_speed_10m").alias("wind_speed_10m"),
        F.col("hourly.weather_code").alias("weather_code"),
    )

    return df_response


# Join response và metadata    
def attach_forecast_metadata(
    df_response: DataFrame,
    df_metadata: DataFrame,
) -> DataFrame:

    df = df_response.join(
        df_metadata,
        on="ingestion_id",
        how="left",
    )

    return df
























