import argparse

import fastorder.ingestion.incremental.table_config as tc
from fastorder.transformation.silver.operational.common import run_operational_silver
from fastorder.transformation.silver.operational.orders import transform_orders


def get_table_configs() -> dict:
    return {
        obj.table_name: obj
        for obj in vars(tc).values()
        if isinstance(obj, tc.IncrementalTableConfig)
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Chạy Operational Silver cho một bảng")
    parser.add_argument("--table", required=True)
    args = parser.parse_args()

    configs = get_table_configs()

    if args.table not in configs:
        raise ValueError(
            f"Bảng không hợp lệ: {args.table}. "
            f"Các bảng hỗ trợ: {sorted(configs)}"
        )

    transforms = {
        "orders": transform_orders,
    }

    run_operational_silver(
        config=configs[args.table],
        transform=transforms.get(args.table),
    )


if __name__ == "__main__":
    main()