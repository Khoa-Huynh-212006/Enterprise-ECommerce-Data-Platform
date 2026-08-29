from datetime import datetime

from fastorder.db.connection import (
    get_engine,
)

from fastorder.ingestion.incremental.table_config import (
    ORDER_ITEMS_CONFIG,
    ORDER_PAYMENTS_CONFIG,
    ORDER_REVIEWS_CONFIG,
)

from fastorder.ingestion.incremental.table_extractor import (
    get_upper_watermark,
    extract_table_batch,
)


TIMESTAMP_FORMAT = (
    "%Y-%m-%dT%H:%M:%S.%f"
)

TIMESTAMP_EPOCH = (
    "1970-01-01T00:00:00.000000"
)


CONFIGS = (
    ORDER_ITEMS_CONFIG,
    ORDER_PAYMENTS_CONFIG,
    ORDER_REVIEWS_CONFIG,
)


def build_initial_watermark(config):

    watermark = {
        config.watermark_column:
            TIMESTAMP_EPOCH,
    }

    for column, value in zip(
        config.primary_key_columns,
        config.initial_primary_key_values,
    ):
        watermark[column] = value

    return watermark


def watermark_key(
    watermark,
    config,
):
    return (
        datetime.strptime(
            watermark[
                config.watermark_column
            ],
            TIMESTAMP_FORMAT,
        ),
        *(
            watermark[column]
            for column
            in config.primary_key_columns
        ),
    )


def primary_key(
    record,
    config,
):
    return tuple(
        record[column]
        for column
        in config.primary_key_columns
    )


engine = get_engine()


with engine.connect() as conn:

    for config in CONFIGS:

        print(
            "\n"
            + "=" * 60
        )

        print(
            f"TEST TABLE: "
            f"{config.table_name}"
        )

        initial = (
            build_initial_watermark(
                config
            )
        )

        upper = get_upper_watermark(
            conn=conn,
            config=config,
        )

        assert upper is not None

        print(
            "Upper:",
            upper,
        )

        batch_1, wm_1 = (
            extract_table_batch(
                conn=conn,
                config=config,
                lower_watermark=initial,
                upper_watermark=upper,
                batch_size=5,
            )
        )

        assert len(batch_1) == 5

        batch_2, wm_2 = (
            extract_table_batch(
                conn=conn,
                config=config,
                lower_watermark=wm_1,
                upper_watermark=upper,
                batch_size=5,
            )
        )

        assert len(batch_2) == 5

        pk_1 = {
            primary_key(
                row,
                config,
            )
            for row in batch_1
        }

        pk_2 = {
            primary_key(
                row,
                config,
            )
            for row in batch_2
        }

        assert pk_1.isdisjoint(
            pk_2
        )

        assert (
            watermark_key(
                wm_1,
                config,
            )
            <
            watermark_key(
                wm_2,
                config,
            )
        )

        print(
            "Batch 1 watermark:",
            wm_1,
        )

        print(
            "Batch 2 watermark:",
            wm_2,
        )

        print(
            f"[PASS] "
            f"{config.table_name}"
        )


print(
    "\n"
    "WAVE 2 COMPOSITE EXTRACTOR: "
    "PERFECT PASS"
)