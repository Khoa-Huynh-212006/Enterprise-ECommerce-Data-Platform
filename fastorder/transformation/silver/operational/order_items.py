from fastorder.ingestion.incremental.table_config import ORDER_ITEMS_CONFIG
from fastorder.transformation.silver.operational.common import run_operational_silver


if __name__ == "__main__":
    run_operational_silver(config=ORDER_ITEMS_CONFIG)