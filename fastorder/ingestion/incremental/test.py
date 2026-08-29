from pathlib import Path
from datetime import datetime

from fastorder.db.connection import get_engine

from fastorder.ingestion.incremental.table_config import (
    CUSTOMERS_CONFIG,
)

from fastorder.ingestion.incremental.table_extractor import (
    get_upper_watermark,
    extract_table_batch,
)

from fastorder.ingestion.incremental.incremental_runner import (
    run_table_incremental_ingestion,
)

from fastorder.ingestion.incremental.checkpoint_manager import (
    save_checkpoint_atomic,
    load_checkpoint,
)


INITIAL_WATERMARK = {
    "updated_at": "1970-01-01T00:00:00.000000",
    "customer_id": "",
}


test_started_at = datetime.now()

test_run_id = (
    "test_customers_generic_"
    + test_started_at.strftime(
        "%Y%m%d%H%M%S%f"
    )
)


checkpoint_path = Path(
    "/tmp/test_customers_checkpoint.json"
)

pending_path = Path(
    "/tmp/test_customers_pending.json"
)


def cleanup():
    for path in [
        checkpoint_path,
        pending_path,
        Path(f"{checkpoint_path}.tmp"),
        Path(f"{pending_path}.tmp"),
    ]:
        if path.exists():
            path.unlink()


cleanup()

engine = get_engine()


try:
    with engine.connect() as conn:

        # --------------------------------
        # 1. Freeze một test boundary
        #    chỉ gồm 12 customers
        # --------------------------------

        full_upper = get_upper_watermark(
            conn=conn,
            config=CUSTOMERS_CONFIG,
        )

        if not full_upper:
            raise RuntimeError(
                "Customers source rong."
            )

        twelve_records, test_upper = (
            extract_table_batch(
                conn=conn,
                config=CUSTOMERS_CONFIG,
                lower_watermark=
                    INITIAL_WATERMARK,
                upper_watermark=
                    full_upper,
                batch_size=12,
            )
        )

        assert len(twelve_records) == 12

        print(
            "Test upper watermark:",
            test_upper,
        )

        # --------------------------------
        # 2. Initial checkpoint
        # --------------------------------

        initial_checkpoint = {
            "version": 1,
            "table_name": "customers",
            "watermark":
                INITIAL_WATERMARK,
        }

        save_checkpoint_atomic(
            checkpoint_path=
                checkpoint_path,
            checkpoint=
                initial_checkpoint,
            expected_table_name=
                "customers",
        )

        # --------------------------------
        # 3. Generic runner
        #
        # 12 rows / batch_size 5
        #
        # Expected:
        # 5 + 5 + 2
        # --------------------------------

        result = (
            run_table_incremental_ingestion(
                conn=conn,
                config=CUSTOMERS_CONFIG,
                checkpoint_path=
                    checkpoint_path,
                pending_context_path=
                    pending_path,
                batch_size=5,
                run_id=test_run_id,
                run_started_at=
                    test_started_at,
                run_upper_watermark=
                    test_upper,
            )
        )

        print(
            "\nFirst run result:",
            result,
        )

        assert (
            result["status"]
            == "run_completed"
        )

        assert (
            result["records_written"]
            == 12
        )

        assert (
            result["batches_committed"]
            == 3
        )

        assert (
            result["final_watermark"]
            == test_upper
        )

        assert not pending_path.exists()

        # --------------------------------
        # 4. Verify checkpoint
        # --------------------------------

        checkpoint = load_checkpoint(
            checkpoint_path,
            "customers",
        )

        assert (
            checkpoint["watermark"]
            == test_upper
        )

        print(
            "\n[PASS] Customers "
            "generic runner wrote "
            "12 records in 3 batches."
        )

        # --------------------------------
        # 5. Replay same boundary
        #
        # Phải NO_OP
        # --------------------------------

        replay_result = (
            run_table_incremental_ingestion(
                conn=conn,
                config=CUSTOMERS_CONFIG,
                checkpoint_path=
                    checkpoint_path,
                pending_context_path=
                    pending_path,
                batch_size=5,
                run_id=
                    f"{test_run_id}_replay",
                run_started_at=
                    datetime.now(),
                run_upper_watermark=
                    test_upper,
            )
        )

        print(
            "\nReplay result:",
            replay_result,
        )

        assert (
            replay_result["status"]
            == "no_new_data"
        )

        assert (
            replay_result[
                "records_written"
            ]
            == 0
        )

        assert (
            replay_result[
                "batches_committed"
            ]
            == 0
        )

        print(
            "\nCUSTOMERS GENERIC "
            "RUNNER E2E TEST: "
            "PERFECT PASS"
        )


finally:
    cleanup()