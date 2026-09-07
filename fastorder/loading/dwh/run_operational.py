import argparse

import fastorder.ingestion.incremental.table_config as tc
from fastorder.loading.dwh.operational import load_operational_table
from fastorder.transformation.common.spark_session import create_spark_session


def get_configs() -> dict:
    return {
        obj.table_name: obj
        for obj in vars(tc).values()
        if isinstance(obj, tc.IncrementalTableConfig)
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument("--table")
    group.add_argument("--all", action="store_true")

    args = parser.parse_args()
    configs = get_configs()

    if args.table and args.table not in configs:
        raise ValueError(
            f"Bảng không hợp lệ: {args.table}. "
            f"Hỗ trợ: {sorted(configs)}"
        )

    tables = list(configs) if args.all else [args.table]

    spark = create_spark_session("fastorder-operational-dwh-load")

    for table in tables:
        load_operational_table(
            spark=spark,
            config=configs[table],
        )

    spark.stop()

    print(
        f"FASTORDER OPERATIONAL → DWH: "
        f"{len(tables)}/{len(tables)} PASS"
    )


if __name__ == "__main__":
    main()