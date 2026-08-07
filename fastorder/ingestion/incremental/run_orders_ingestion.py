from pathlib import Path 
from fastorder.ingestion.incremental.incremental_runner import run_orders_incremental_ingestion
from datetime import datetime
from fastorder.db.connection import get_engine
import json

def run_orders_ingestion():
    PROJECT_ROOT = Path(__file__).resolve().parents[3]
    BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze"
    CHECKPOINT_PATH = PROJECT_ROOT / "state" / "checkpoints" / "orders_checkpoint.json"
    PENDING_PATH = PROJECT_ROOT / "state" / "pending" / "orders_pending_batch.json"

    BRONZE_ROOT.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    PENDING_PATH.parent.mkdir(parents=True, exist_ok=True)

    RUN_STARTED_AT = datetime.now()
    run_timestamp_str = RUN_STARTED_AT.strftime("%Y%m%d_%H%M%S_%f")
    RUN_ID = f"orders_{run_timestamp_str}"
    BATCH_SIZE = 5000
    engine = get_engine()

    print(f" BẮT ĐẦU INCREMENTAL INGESTION RUN")
    print(f"-> Run ID      : {RUN_ID}")
    print(f"-> Started At  : {RUN_STARTED_AT}")
    print(f"-> Batch Size  : {BATCH_SIZE}")
    print(f"-> Bronze Root : {BRONZE_ROOT}")
    print(f"-> Checkpoint  : {CHECKPOINT_PATH}")

    with engine.connect() as conn:
        result = run_orders_incremental_ingestion(
            conn=conn,
            bronze_root=BRONZE_ROOT,
            checkpoint_path=CHECKPOINT_PATH,
            pending_context_path=PENDING_PATH,
            batch_size=BATCH_SIZE,
            run_id=RUN_ID,
            run_started_at=RUN_STARTED_AT
        )

    print("\nKết quả thực thi")
    print(json.dumps(result, indent=4, ensure_ascii=False, default=str))
    tmp_files = list(BRONZE_ROOT.rglob("*.tmp"))
    if tmp_files:
        print(f"\nCẢNH BÁO: Phát hiện file .tmp chưa được dọn dẹp: {tmp_files}")
    else:
        print("\nData Lake sạch sẽ, không tồn dư file rác.")

if __name__ == "__main__":
    run_orders_ingestion()