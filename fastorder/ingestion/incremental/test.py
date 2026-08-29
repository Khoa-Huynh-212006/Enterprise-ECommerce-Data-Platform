from fastorder.ingestion.incremental.checkpoint_manager import (
    build_initial_checkpoint,
    validate_checkpoint,
)


checkpoint = (
    build_initial_checkpoint(
        "orders"
    )
)

print(checkpoint)

validate_checkpoint(
    checkpoint,
    "orders",
)

print("Generic Orders Checkpoint: PASS")