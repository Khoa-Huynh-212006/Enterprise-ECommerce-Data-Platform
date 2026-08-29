import io
import json
from pathlib import Path

import pandas as pd

from fastorder.storage.adls_client import (
    get_bronze_file_system_client,
)


CHECKPOINT_PATH = Path(
    "/opt/airflow/state/checkpoints/"
    "orders_checkpoint.json"
)


def main():

    print(
        "=" * 80
    )
    print(
        "ORDERS BRONZE DIAGNOSTIC"
    )
    print(
        "=" * 80
    )

    # -------------------------------------------------
    # 1. Checkpoint
    # -------------------------------------------------

    print(
        "\n[1] CURRENT CHECKPOINT"
    )

    if CHECKPOINT_PATH.exists():

        checkpoint = json.loads(
            CHECKPOINT_PATH.read_text()
        )

        print(
            json.dumps(
                checkpoint,
                indent=4,
                ensure_ascii=False,
            )
        )

    else:

        print(
            "Checkpoint không tồn tại."
        )

    # -------------------------------------------------
    # 2. Bronze files
    # -------------------------------------------------

    fs_client = (
        get_bronze_file_system_client()
    )

    paths = [
        path.name
        for path in fs_client.get_paths(
            path="orders",
            recursive=True,
        )
        if (
            not path.is_directory
            and path.name.endswith(
                "/part-000.parquet"
            )
        )
    ]

    print(
        "\n[2] BRONZE FILES"
    )

    print(
        f"Total parquet files: "
        f"{len(paths)}"
    )

    summary = []

    all_order_ids = set()

    production_order_ids = set()

    test_order_ids = set()

    for path in sorted(paths):

        parts = path.split("/")

        extraction_part = next(
            (
                part
                for part in parts
                if part.startswith(
                    "extraction_id="
                )
            ),
            None,
        )

        if extraction_part is None:

            extraction_id = (
                "<UNKNOWN>"
            )

        else:

            extraction_id = (
                extraction_part.split(
                    "=",
                    1,
                )[1]
            )

        is_test = (
            extraction_id.startswith(
                "test_"
            )
        )

        raw = (
            fs_client
            .get_file_client(path)
            .download_file()
            .readall()
        )

        df = pd.read_parquet(
            io.BytesIO(raw)
        )

        order_ids = set(
            df["order_id"]
            .astype(str)
            .tolist()
        )

        all_order_ids.update(
            order_ids
        )

        if is_test:

            test_order_ids.update(
                order_ids
            )

        else:

            production_order_ids.update(
                order_ids
            )

        if len(df) > 0:

            min_updated_at = (
                pd.to_datetime(
                    df["updated_at"]
                ).min()
            )

            max_updated_at = (
                pd.to_datetime(
                    df["updated_at"]
                ).max()
            )

        else:

            min_updated_at = None
            max_updated_at = None

        summary.append(
            {
                "extraction_id":
                    extraction_id,

                "is_test":
                    is_test,

                "rows":
                    len(df),

                "distinct_orders":
                    len(order_ids),

                "min_updated_at":
                    min_updated_at,

                "max_updated_at":
                    max_updated_at,

                "path":
                    path,
            }
        )

    # -------------------------------------------------
    # 3. Per-file result
    # -------------------------------------------------

    print(
        "\n[3] PER FILE"
    )

    for item in summary:

        print(
            "\n"
            f"Extraction : "
            f"{item['extraction_id']}\n"

            f"Test       : "
            f"{item['is_test']}\n"

            f"Rows       : "
            f"{item['rows']}\n"

            f"Orders     : "
            f"{item['distinct_orders']}\n"

            f"Min updated: "
            f"{item['min_updated_at']}\n"

            f"Max updated: "
            f"{item['max_updated_at']}\n"

            f"Path       : "
            f"{item['path']}"
        )

    # -------------------------------------------------
    # 4. Overall summary
    # -------------------------------------------------

    print(
        "\n"
        + "=" * 80
    )

    print(
        "SUMMARY"
    )

    print(
        "=" * 80
    )

    print(
        f"All Bronze distinct orders : "
        f"{len(all_order_ids)}"
    )

    print(
        f"Production distinct orders : "
        f"{len(production_order_ids)}"
    )

    print(
        f"Test distinct orders       : "
        f"{len(test_order_ids)}"
    )

    print(
        f"Production parquet files   : "
        f"{sum(not x['is_test'] for x in summary)}"
    )

    print(
        f"Test parquet files         : "
        f"{sum(x['is_test'] for x in summary)}"
    )


if __name__ == "__main__":
    main()