from fastorder.db.connection import (
    get_engine,
)

from fastorder.ingestion.incremental.table_config import (
    WAREHOUSES_CONFIG,
    PRODUCT_CATEGORY_TRANSLATION_CONFIG,
    SELLERS_CONFIG,
    PRODUCTS_CONFIG,
    GEOLOCATION_CONFIG,
)

from fastorder.ingestion.incremental.table_extractor import (
    get_upper_watermark,
    extract_table_batch,
)


TIMESTAMP_EPOCH = (
    "1970-01-01T00:00:00.000000"
)


CONFIGS = (
    WAREHOUSES_CONFIG,
    PRODUCT_CATEGORY_TRANSLATION_CONFIG,
    SELLERS_CONFIG,
    PRODUCTS_CONFIG,
    GEOLOCATION_CONFIG,
)


def build_initial_watermark(
    config,
):
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


def get_pk(
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

        initial_watermark = (
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
                lower_watermark=
                    initial_watermark,
                upper_watermark=upper,
                batch_size=2,
            )
        )

        assert len(batch_1) > 0

        print(
            "Batch 1:",
            len(batch_1),
            "Next:",
            wm_1,
        )

        batch_2, wm_2 = (
            extract_table_batch(
                conn=conn,
                config=config,
                lower_watermark=wm_1,
                upper_watermark=upper,
                batch_size=2,
            )
        )

        if batch_2:

            pk_1 = {
                get_pk(row, config)
                for row in batch_1
            }

            pk_2 = {
                get_pk(row, config)
                for row in batch_2
            }

            assert pk_1.isdisjoint(
                pk_2
            )

            assert wm_2 != wm_1

            print(
                "Batch 2:",
                len(batch_2),
                "Next:",
                wm_2,
            )

        print(
            f"[PASS] "
            f"{config.table_name}"
        )


print(
    "\n"
    "WAVE 1 GENERIC EXTRACTOR: "
    "PERFECT PASS"
)