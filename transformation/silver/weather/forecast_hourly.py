from pyspark.sql import DataFrame, SparkSession
from delta.tables import DeltaTable


def load_forecast_bronze(
    spark: SparkSession, 
    ingestion_path: str
)-> tuple[DataFrame, DataFrame]:
    if not path_exists(f"{ingestion_path}/_SUCCESS"):
        raise ValueError(f"Bronze ingestion path này chưa commit: {ingestion_path}")
    
    df_metadata = spark.read.format("json").option("multiline", True).load(f"{ingestion_path}/metadata.json")
    df_response = spark.read.format("json").option("multiline", True).load(f"{ingestion_path}/response.json")
    return df_response, df_metadata
    

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














