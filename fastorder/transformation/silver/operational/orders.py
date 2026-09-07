from pyspark.sql import DataFrame, functions as F

from fastorder.ingestion.incremental.table_config import ORDERS_CONFIG
from fastorder.transformation.silver.operational.common import run_operational_silver


def transform_orders(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "order_purchase_date",
        F.to_date("order_purchase_timestamp")
    )


if __name__ == "__main__":
    run_operational_silver(
        config=ORDERS_CONFIG,
        transform=transform_orders
    )