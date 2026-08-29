from fastorder.db.connection import (
    get_engine,
)

from fastorder.ingestion.incremental.table_config import (
    CUSTOMERS_CONFIG,
)

from fastorder.ingestion.incremental.table_extractor import (
    get_upper_watermark,
    extract_table_batch,
)


INITIAL_WATERMARK = {
    "updated_at":
        "1970-01-01T00:00:00.000000",
    "customer_id":
        "",
}


engine = get_engine()


with engine.connect() as conn:

    upper = get_upper_watermark(
        conn=conn,
        config=CUSTOMERS_CONFIG,
    )

    print(
        "Upper watermark:",
        upper,
    )

    batch_1, wm_1 = extract_table_batch(
        conn=conn,
        config=CUSTOMERS_CONFIG,
        lower_watermark=
            INITIAL_WATERMARK,
        upper_watermark=upper,
        batch_size=5,
    )

    print(
        "\nBatch 1:",
        len(batch_1),
    )

    print(
        "Batch 1 next watermark:",
        wm_1,
    )

    batch_2, wm_2 = extract_table_batch(
        conn=conn,
        config=CUSTOMERS_CONFIG,
        lower_watermark=wm_1,
        upper_watermark=upper,
        batch_size=5,
    )

    print(
        "\nBatch 2:",
        len(batch_2),
    )

    print(
        "Batch 2 next watermark:",
        wm_2,
    )

    ids_1 = {
        row["customer_id"]
        for row in batch_1
    }

    ids_2 = {
        row["customer_id"]
        for row in batch_2
    }

    assert len(batch_1) == 5
    assert len(batch_2) == 5

    assert ids_1.isdisjoint(
        ids_2
    )

    assert (
        wm_1["updated_at"]
        == wm_2["updated_at"]
    )

    assert (
        wm_2["customer_id"]
        > wm_1["customer_id"]
    )

    print(
        "\nCUSTOMERS GENERIC "
        "EXTRACTOR TEST: PASS"
    )