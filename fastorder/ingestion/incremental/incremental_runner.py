from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional
from sqlalchemy import Connection

from fastorder.ingestion.incremental.orders_extractor import extract_orders_batch
from fastorder.ingestion.incremental.bronze_writer import write_bronze_batch
from fastorder.ingestion.incremental.checkpoint_manager import save_checkpoint_atomic, load_checkpoint
from fastorder.ingestion.incremental.pending_batch_manager import (
    build_pending_batch_context,
    save_pending_batch_context_atomic,
    delete_pending_batch_context,
    load_pending_batch_context
)

def _extract_batch_number(extraction_id: str) -> int:
    marker = "_batch_"
    if marker not in extraction_id:
        raise RuntimeError(f"extraction_id sai dinh dang: {extraction_id}")
    
    suffix = extraction_id.rsplit(marker, 1)[-1]
    
    try:
        batch_number = int(suffix)
    except ValueError as error:
        raise RuntimeError(f"Khong doc duoc batch number tu: {extraction_id}") from error

    if batch_number <= 0:
        raise RuntimeError("Batch number trong extraction_id phai lon hon 0.")

    return batch_number


def process_one_orders_batch(
        conn: Connection,
        lower_watermark: Dict[str, str],
        run_upper_watermark: Dict[str, str],
        batch_size: int, 
        bronze_root: Path, 
        checkpoint_path: Path,
        pending_context_path: Path,
        run_id: str,
        extraction_id: str,
        ingested_at: datetime,
        extraction_upper_watermark: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    
    effective_extraction_upper = (
        extraction_upper_watermark 
        if extraction_upper_watermark is not None 
        else run_upper_watermark
    )
    
    batch_records, next_watermark = extract_orders_batch(
        lower_watermark=lower_watermark,
        upper_watermark=effective_extraction_upper,
        batch_size=batch_size,
        conn=conn
    )

    if not batch_records:
        return {
            "status": "completed",
            "records_written": 0,
            "checkpoint_updated": False,
            "pending_context_deleted": False
        }

    pending_context = build_pending_batch_context(
        table_name="orders",
        run_id=run_id,
        run_upper_watermark=run_upper_watermark,
        lower_watermark=lower_watermark,
        batch_upper_watermark=next_watermark,
        extraction_id=extraction_id,
        ingested_at=ingested_at.strftime("%Y-%m-%dT%H:%M:%S.%f"),
        batch_size=batch_size
    )
    
    save_pending_batch_context_atomic(
        file_path=pending_context_path,
        context=pending_context,
        expected_table_name="orders"
    )

    output_path = write_bronze_batch(
        records=batch_records,
        bronze_root=bronze_root,
        table_name="orders",
        extraction_id=extraction_id,
        ingested_at=ingested_at
    )

    new_checkpoint = {
        "version": 1,
        "table_name": "orders",
        "watermark": next_watermark
    }

    save_checkpoint_atomic(
        checkpoint_path=checkpoint_path, 
        checkpoint=new_checkpoint,
        expected_table_name="orders"
    )
    
    delete_pending_batch_context(pending_context_path)

    return {
        "status": "batch_committed",
        "records_written": len(batch_records),
        "output_path": output_path,
        "next_watermark": next_watermark,
        "checkpoint_updated": True,
        "pending_context_deleted": True
    }


def run_orders_incremental_ingestion(
    conn: Connection,
    bronze_root: Path,
    checkpoint_path: Path,
    pending_context_path: Path,
    batch_size: int,
    run_id: str,
    run_started_at: datetime,
    run_upper_watermark: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    
    if (
        not isinstance(batch_size, int)
        or isinstance(batch_size, bool)
        or batch_size <= 0
    ):
        raise ValueError("batch_size phai la so nguyen > 0.")
        
    if not run_id:
        raise ValueError("run_id khong duoc de trong.")
    if not isinstance(run_started_at, datetime):
        raise ValueError("run_started_at phai la datetime object.")

    checkpoint = load_checkpoint(checkpoint_path, "orders")
    current_lower = checkpoint["watermark"]
    pending_context = load_pending_batch_context(pending_context_path, "orders")

    effective_run_id = run_id
    effective_run_started_at = run_started_at
    effective_batch_size = batch_size

    if pending_context:
        effective_run_id = pending_context["run_id"]
        effective_run_started_at = datetime.strptime(
            pending_context["ingested_at"], 
            "%Y-%m-%dT%H:%M:%S.%f"
        )
        effective_batch_size = pending_context["batch_size"]
        run_upper_watermark = pending_context["run_upper_watermark"]
    else:
        if run_upper_watermark is None:
            from fastorder.ingestion.incremental.orders_extractor import get_upper_watermark
            run_upper_watermark = get_upper_watermark(conn)

        if not run_upper_watermark:
            return {
                "status": "source_empty",
                "run_id": effective_run_id,
                "batches_committed": 0,
                "records_written": 0,
                "final_watermark": current_lower,
                "output_paths": []
            }

    def _wm_key(wm: Dict[str, str]):
        return (wm["updated_at"], wm["order_id"])

    curr_key = _wm_key(current_lower)
    upper_key = _wm_key(run_upper_watermark)

    next_batch_number = 1
    batches_committed_this_invocation = 0
    total_records = 0
    output_paths = []

    # CRASH RECOVERY (PHỤC HỒI TRẠNG THÁI)
    
    if pending_context:
        p_lower_key = _wm_key(pending_context["lower_watermark"])
        p_upper_key = _wm_key(pending_context["batch_upper_watermark"])

        if curr_key == p_lower_key:
            result = process_one_orders_batch(
                conn=conn,
                lower_watermark=pending_context["lower_watermark"],
                run_upper_watermark=run_upper_watermark,
                batch_size=effective_batch_size,
                bronze_root=bronze_root,
                checkpoint_path=checkpoint_path,
                pending_context_path=pending_context_path,
                run_id=effective_run_id,
                extraction_id=pending_context["extraction_id"],
                ingested_at=effective_run_started_at,
                extraction_upper_watermark=pending_context["batch_upper_watermark"]
            )

            if result["status"] == "completed":
                raise RuntimeError("Batch phuc hoi tra ve rong. Du lieu nguon co the da bi thay doi bat thuong.")
            
            if result["next_watermark"] != pending_context["batch_upper_watermark"]:
                raise RuntimeError("Batch phuc hoi khong ket thuc dung batch upper da luu.")
                
            current_lower = result["next_watermark"]
            curr_key = _wm_key(current_lower)
            total_records += result["records_written"]
            output_paths.append(result["output_path"])
            batches_committed_this_invocation += 1
            
            next_batch_number = _extract_batch_number(pending_context["extraction_id"]) + 1
                
        elif curr_key == p_upper_key:
            delete_pending_batch_context(pending_context_path)
            next_batch_number = _extract_batch_number(pending_context["extraction_id"]) + 1
        else:
            raise RuntimeError(f"Trang thai mau thuan: Checkpoint {current_lower} khong khop voi Pending Context.")

    if curr_key == upper_key and total_records == 0:
        return {
            "status": "no_new_data",
            "run_id": effective_run_id,
            "batches_committed": 0,
            "records_written": 0,
            "final_watermark": current_lower,
            "output_paths": []
        }
        
    if curr_key > upper_key:
        raise RuntimeError(
            f"Trang thai khong hop le: Lower watermark ({current_lower}) "
            f"lon hon Upper watermark ({run_upper_watermark})."
        )

    
    # MULTI-BATCH PROCESSING
    
    while curr_key < upper_key:
        extraction_id = f"{effective_run_id}_batch_{next_batch_number:06d}"
        
        result = process_one_orders_batch(
            conn=conn,
            lower_watermark=current_lower,
            run_upper_watermark=run_upper_watermark,
            batch_size=effective_batch_size,
            bronze_root=bronze_root,
            checkpoint_path=checkpoint_path,
            pending_context_path=pending_context_path,
            run_id=effective_run_id,
            extraction_id=extraction_id,
            ingested_at=effective_run_started_at
        )

        if result["status"] == "completed":
            raise RuntimeError(
                "Extractor tra ve batch rong du current watermark "
                "van nho hon run upper watermark. (Loi Data Anomaly)"
            )
            
        next_watermark = result["next_watermark"]
        next_key = _wm_key(next_watermark)

        if next_key <= curr_key:
            raise RuntimeError(
                f"Runner khong tien len! Next watermark ({next_watermark}) "
                f"khong lon hon Lower watermark hien tai ({current_lower})."
            )

        current_lower = next_watermark
        curr_key = next_key
        
        total_records += result["records_written"]
        output_paths.append(result["output_path"])
        
        next_batch_number += 1
        batches_committed_this_invocation += 1

    return {
        "status": "run_completed",
        "run_id": effective_run_id,
        "run_upper_watermark": run_upper_watermark,
        "final_watermark": current_lower,
        "batches_committed": batches_committed_this_invocation,
        "records_written": total_records,
        "output_paths": output_paths
    }



# SMOKE TEST

if __name__ == "__main__":
    import shutil
    import pandas as pd
    from unittest.mock import patch
    from fastorder.db.connection import get_engine
    from fastorder.ingestion.incremental.orders_extractor import get_upper_watermark

    print("Bat dau Smoke Test: Incremental Runner (Crash Recovery Full Suite)\n" + "-"*50)

    test_bronze_root = Path("test_bronze_pending")
    test_checkpoint_path = Path("test_multi_checkpoint.json")
    test_pending_context_path = Path("test_multi_pending.json")

    def _cleanup():
        if test_bronze_root.exists(): shutil.rmtree(test_bronze_root)
        for p in [test_checkpoint_path, test_pending_context_path, Path(f"{test_pending_context_path}.tmp")]:
            if p.exists(): p.unlink()

    engine = get_engine()

    try:
        with engine.connect() as conn:
            full_upper = get_upper_watermark(conn)
            if not full_upper:
                raise RuntimeError("Bang rong, khong du du lieu de test!")
                
            initial_lower_wm = {"updated_at": "1970-01-01T00:00:00.000000", "order_id": ""}
            
            twelve_records, test_upper = extract_orders_batch(
                conn=conn, lower_watermark=initial_lower_wm, upper_watermark=full_upper, batch_size=12
            )
            
            if len(twelve_records) != 12:
                raise RuntimeError(f"Smoke test can dung 12 records, nhung chi tra ve {len(twelve_records)}.")

            
            # TEST 1: SINGLE BATCH HAPPY PATH
            
            _cleanup()
            save_checkpoint_atomic(test_checkpoint_path, {"version": 1, "table_name": "orders", "watermark": initial_lower_wm}, "orders")
            
            print("\n[TEST 1] Chay thu 1 Batch 5 records...")
            result_single = process_one_orders_batch(
                conn=conn,
                lower_watermark=initial_lower_wm,
                run_upper_watermark=test_upper,
                batch_size=5,
                bronze_root=test_bronze_root,
                checkpoint_path=test_checkpoint_path,
                pending_context_path=test_pending_context_path,
                run_id="test_single_run",
                extraction_id="test_single_run_batch_001",
                ingested_at=datetime.now()
            )
            
            assert result_single["status"] == "batch_committed"
            assert not test_pending_context_path.exists()
            print("  [PASS] Single Batch hoan tat, Pending Context da duoc don sach.")

            
            # TEST 2: MULTI-BATCH 5-5-2
            
            _cleanup()
            save_checkpoint_atomic(test_checkpoint_path, {"version": 1, "table_name": "orders", "watermark": initial_lower_wm}, "orders")
            
            print("\n[TEST 2] Chay Multi-Batch Runner (Batch_size = 5, Max 12 records)...")
            run_result = run_orders_incremental_ingestion(
                conn=conn,
                bronze_root=test_bronze_root,
                checkpoint_path=test_checkpoint_path,
                pending_context_path=test_pending_context_path,
                batch_size=5,
                run_id="test_multi_run_2026",
                run_started_at=datetime.now(),
                run_upper_watermark=test_upper
            )

            assert run_result["status"] == "run_completed"
            assert run_result["records_written"] == 12
            assert run_result["batches_committed"] == 3
            assert not test_pending_context_path.exists()
            
            final_checkpoint = load_checkpoint(test_checkpoint_path, "orders")
            assert final_checkpoint["watermark"] == test_upper
            print("  [PASS] Multi-Batch (5-5-2) hoan tat, Checkpoint cuoi khop test_upper.")

            
            # TEST 3: CRASH SAU KHI GHI BRONZE, TRUOC CHECKPOINT
            
            _cleanup()
            save_checkpoint_atomic(test_checkpoint_path, {"version": 1, "table_name": "orders", "watermark": initial_lower_wm}, "orders")
            
            print("\n[TEST 3] Gia lap Crash sau Bronze, truoc Checkpoint...")
            with patch("__main__.save_checkpoint_atomic", side_effect=RuntimeError("Gia lap mat dien")):
                try:
                    run_orders_incremental_ingestion(
                        conn=conn,
                        bronze_root=test_bronze_root,
                        checkpoint_path=test_checkpoint_path,
                        pending_context_path=test_pending_context_path,
                        batch_size=5,
                        run_id="test_crash_1",
                        run_started_at=datetime.now(),
                        run_upper_watermark=test_upper
                    )
                except RuntimeError as e:
                    assert "Gia lap mat dien" in str(e)
            
            saved_ckpt = load_checkpoint(test_checkpoint_path, "orders")
            assert saved_ckpt["watermark"] == initial_lower_wm
            assert test_pending_context_path.exists()
            assert len(list(test_bronze_root.rglob("*.parquet"))) == 1
            print("  [PASS] He thong bat loi thanh cong, Checkpoint giu nguyen, Pending Context ton tai.")

            print("  [TEST 3] Bat dau chay lai (Resume) de phuc hoi...")
            recovery_result = run_orders_incremental_ingestion(
                conn=conn,
                bronze_root=test_bronze_root,
                checkpoint_path=test_checkpoint_path,
                pending_context_path=test_pending_context_path,
                batch_size=999,  # Co tinh truyen batch_size khac de kiem tra viec ke thua tu pending
                run_id="test_crash_1_new", 
                run_started_at=datetime.now(),
                run_upper_watermark=test_upper
            )
            
            assert recovery_result["records_written"] == 12
            assert recovery_result["batches_committed"] == 3
            assert not test_pending_context_path.exists()
            assert recovery_result["run_id"] == "test_crash_1" # Kiem tra Stable Run Identity
            
            final_checkpoint_3 = load_checkpoint(test_checkpoint_path, "orders")
            assert final_checkpoint_3["watermark"] == test_upper
            
            parquet_files_3 = list(test_bronze_root.rglob("*.parquet"))
            assert len(parquet_files_3) == 3
            
            actual_ids_3 = []
            for path in sorted(parquet_files_3):
                df = pd.read_parquet(path)
                actual_ids_3.extend(df["order_id"].tolist())
            expected_ids = [record["order_id"] for record in twelve_records]
            assert actual_ids_3 == expected_ids
            assert len(actual_ids_3) == 12
            assert len(set(actual_ids_3)) == 12
            assert not list(test_bronze_root.rglob("*.tmp"))
            print("  [PASS] Phuc hoi thanh cong, du lieu khop 100%, Batch Size duoc ke thua.")

            
            # TEST 4: CRASH SAU CHECKPOINT, TRUOC KHI XOA PENDING
            
            _cleanup()
            save_checkpoint_atomic(test_checkpoint_path, {"version": 1, "table_name": "orders", "watermark": initial_lower_wm}, "orders")
            
            print("\n[TEST 4] Gia lap Crash sau Checkpoint, truoc khi xoa Pending...")
            with patch("__main__.delete_pending_batch_context", side_effect=RuntimeError("Gia lap tat nguon")):
                try:
                    run_orders_incremental_ingestion(
                        conn=conn,
                        bronze_root=test_bronze_root,
                        checkpoint_path=test_checkpoint_path,
                        pending_context_path=test_pending_context_path,
                        batch_size=5,
                        run_id="test_crash_2",
                        run_started_at=datetime.now(),
                        run_upper_watermark=test_upper
                    )
                except RuntimeError as e:
                    assert "Gia lap tat nguon" in str(e)

            saved_ckpt = load_checkpoint(test_checkpoint_path, "orders")
            assert saved_ckpt["watermark"] != initial_lower_wm
            assert test_pending_context_path.exists()
            print("  [PASS] He thong dinh Crash, Checkpoint da tien len, Pending Context thanh rac.")

            print("  [TEST 4] Bat dau chay lai de don rac va hoan tat tien trinh...")
            recovery_result_2 = run_orders_incremental_ingestion(
                conn=conn,
                bronze_root=test_bronze_root,
                checkpoint_path=test_checkpoint_path,
                pending_context_path=test_pending_context_path,
                batch_size=999, 
                run_id="test_crash_2_new",
                run_started_at=datetime.now(),
                run_upper_watermark=test_upper
            )
            
            assert recovery_result_2["records_written"] == 7
            assert recovery_result_2["batches_committed"] == 2
            assert not test_pending_context_path.exists()
            assert recovery_result_2["run_id"] == "test_crash_2"
            
            final_checkpoint_4 = load_checkpoint(test_checkpoint_path, "orders")
            assert final_checkpoint_4["watermark"] == test_upper
            
            parquet_files_4 = list(test_bronze_root.rglob("*.parquet"))
            assert len(parquet_files_4) == 3
            
            actual_ids_4 = []
            for path in sorted(parquet_files_4):
                df = pd.read_parquet(path)
                actual_ids_4.extend(df["order_id"].tolist())
            assert actual_ids_4 == expected_ids
            assert len(actual_ids_4) == 12
            assert len(set(actual_ids_4)) == 12
            assert not list(test_bronze_root.rglob("*.tmp"))
            print("  [PASS] Quet sach Pending rac, hoan thanh 2 Batch cuoi, du lieu khop 100%.")

    finally:
        _cleanup()
        print("\nHoan tat don dep file test.")