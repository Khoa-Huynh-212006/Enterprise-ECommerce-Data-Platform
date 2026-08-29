from fastorder.ingestion.incremental.pending_batch_manager import (
    build_pending_batch_context,
    validate_pending_batch_context,
)


context = build_pending_batch_context(
    table_name="orders",

    run_id="test_run",

    run_upper_watermark={
        "updated_at":
            "2026-08-10T12:00:00.000000",
        "order_id":
            "ffffffffffffffffffffffffffffffff",
    },

    lower_watermark={
        "updated_at":
            "2026-08-10T10:00:00.000000",
        "order_id":
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    },

    batch_upper_watermark={
        "updated_at":
            "2026-08-10T11:00:00.000000",
        "order_id":
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    },

    extraction_id="test_batch_001",

    ingested_at=
        "2026-08-10T12:01:00.000000",

    batch_size=5000,
)


validate_pending_batch_context(
    context,
    "orders",
)

print(
    "Generic Orders Pending Context: PASS"
)