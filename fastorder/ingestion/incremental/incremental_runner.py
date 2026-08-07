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
        ingested_at: datetime
) -> Dict[str, Any]:
    
    # 1. Trích xuất
    batch_records, next_watermark = extract_orders_batch(
        lower_watermark=lower_watermark,
        upper_watermark=run_upper_watermark,
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

    # 2. Xây dựng và Lưu Pending Context
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

    # 3. Ghi Bronze Parquet
    output_path = write_bronze_batch(
        records=batch_records,
        bronze_root=bronze_root,
        table_name="orders",
        extraction_id=extraction_id,
        ingested_at=ingested_at
    )

    # 4. Ghi Checkpoint (Commit)
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
    
    # 5. Xóa Pending Context
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
    
    # 1. Validate Input 
    if (
        not isinstance(batch_size, int)
        or isinstance(batch_size, bool)
        or batch_size <= 0
    ):
        raise ValueError("batch_size phải là số nguyên > 0.")
        
    if not run_id:
        raise ValueError("run_id không được để trống.")
    if not isinstance(run_started_at, datetime):
        raise ValueError("run_started_at phải là datetime object.")

    # 2. Khởi tạo mốc chạy (Checkpoint & Pending)
    checkpoint = load_checkpoint(checkpoint_path, "orders")
    current_lower = checkpoint["watermark"]
    pending_context = load_pending_batch_context(pending_context_path, "orders")

    # 3. Chụp Upper Watermark (1 lần duy nhất)
    from fastorder.ingestion.incremental.orders_extractor import get_upper_watermark
    if run_upper_watermark is None:
        run_upper_watermark = get_upper_watermark(conn)

    if not run_upper_watermark:
        return {
            "status": "source_empty",
            "run_id": run_id,
            "batches_committed": 0,
            "records_written": 0,
            "final_watermark": current_lower,
            "output_paths": []
        }

    def _wm_key(wm: Dict[str, str]):
        return (wm["updated_at"], wm["order_id"])

    curr_key = _wm_key(current_lower)
    upper_key = _wm_key(run_upper_watermark)

    batch_number = 1
    total_records = 0
    output_paths = []

    
    # CRASH RECOVERY (PHỤC HỒI TRẠNG THÁI)
    
    if pending_context:
        p_lower_key = _wm_key(pending_context["lower_watermark"])
        p_upper_key = _wm_key(pending_context["batch_upper_watermark"])

        if curr_key == p_lower_key:
            # Trường hợp 1: Crash sau Pending, trước Checkpoint. Cần retry đúng batch này.
            pending_ingested_at = datetime.strptime(pending_context["ingested_at"], "%Y-%m-%dT%H:%M:%S.%f")
            
            # Ghi đè run_upper_watermark bằng giá trị từ pending để giữ nguyên biên của batch
            run_upper_watermark = pending_context["run_upper_watermark"]
            upper_key = _wm_key(run_upper_watermark)
            
            result = process_one_orders_batch(
                conn=conn,
                lower_watermark=pending_context["lower_watermark"],
                run_upper_watermark=run_upper_watermark,
                batch_size=pending_context["batch_size"],
                bronze_root=bronze_root,
                checkpoint_path=checkpoint_path,
                pending_context_path=pending_context_path,
                run_id=pending_context["run_id"],
                extraction_id=pending_context["extraction_id"],
                ingested_at=pending_ingested_at
            )

            if result["status"] == "completed":
                raise RuntimeError("Batch phục hồi trả về rỗng. Dữ liệu nguồn có thể đã bị thay đổi bất thường.")
                
            current_lower = result["next_watermark"]
            curr_key = _wm_key(current_lower)
            total_records += result["records_written"]
            output_paths.append(result["output_path"])
            
            # Khôi phục số thứ tự batch để các batch sau không bị lặp ID
            try:
                batch_number = int(pending_context["extraction_id"].split("_batch_")[-1]) + 1
            except ValueError:
                batch_number += 1
                
        elif curr_key >= p_upper_key:
            # Trường hợp 2: Crash sau Checkpoint, trước khi xóa Pending. Checkpoint đã an toàn.
            delete_pending_batch_context(pending_context_path)
            
        else:
            raise RuntimeError(f"Trạng thái mâu thuẫn: Checkpoint {current_lower} không khớp với Pending Context.")

    if curr_key == upper_key and total_records == 0:
        return {
            "status": "no_new_data",
            "run_id": run_id,
            "batches_committed": 0,
            "records_written": 0,
            "final_watermark": current_lower,
            "output_paths": []
        }
        
    if curr_key > upper_key:
        raise RuntimeError(
            f"Trạng thái không hợp lệ: Lower watermark ({current_lower}) "
            f"lớn hơn Upper watermark ({run_upper_watermark})."
        )

    # 4. Vòng lặp Multi-batch (Bình thường)
    while curr_key < upper_key:
        extraction_id = f"{run_id}_batch_{batch_number:06d}"
        
        result = process_one_orders_batch(
            conn=conn,
            lower_watermark=current_lower,
            run_upper_watermark=run_upper_watermark,
            batch_size=batch_size,
            bronze_root=bronze_root,
            checkpoint_path=checkpoint_path,
            pending_context_path=pending_context_path,
            run_id=run_id,
            extraction_id=extraction_id,
            ingested_at=run_started_at
        )

        if result["status"] == "completed":
            raise RuntimeError(
                "Extractor trả về batch rỗng dù current watermark "
                "vẫn nhỏ hơn run upper watermark. (Lỗi Data Anomaly)"
            )
            
        next_watermark = result["next_watermark"]
        next_key = _wm_key(next_watermark)

        if next_key <= curr_key:
            raise RuntimeError(
                f"Runner không tiến lên! Next watermark ({next_watermark}) "
                f"không lớn hơn Lower watermark hiện tại ({current_lower})."
            )

        current_lower = next_watermark
        curr_key = next_key
        
        total_records += result["records_written"]
        output_paths.append(result["output_path"])
        batch_number += 1

    return {
        "status": "run_completed",
        "run_id": run_id,
        "run_upper_watermark": run_upper_watermark,
        "final_watermark": current_lower,
        "batches_committed": batch_number - 1,
        "records_written": total_records,
        "output_paths": output_paths
    }



if __name__ == "__main__":
    import shutil
    import pandas as pd
    from unittest.mock import patch
    from fastorder.db.connection import get_engine
    from fastorder.ingestion.incremental.orders_extractor import get_upper_watermark

    print("Bắt đầu Smoke Test: Tích hợp Pending Context & Crash Recovery\n" + "-"*50)

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
                raise RuntimeError("Bảng rỗng, không đủ dữ liệu để test!")
                
            initial_lower_wm = {"updated_at": "1970-01-01T00:00:00.000000", "order_id": ""}
            
            twelve_records, test_upper = extract_orders_batch(
                conn=conn, lower_watermark=initial_lower_wm, upper_watermark=full_upper, batch_size=12
            )
            
            if len(twelve_records) != 12:
                raise RuntimeError(f"Smoke test cần đúng 12 records, nhưng chỉ trả về {len(twelve_records)}.")

            
            # TEST 1: SINGLE BATCH
            
            _cleanup()
            save_checkpoint_atomic(test_checkpoint_path, {"version": 1, "table_name": "orders", "watermark": initial_lower_wm}, "orders")
            
            print("\n[TEST 1] Chạy thử 1 Batch 5 records...")
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
            print("  [PASS] Single Batch hoàn tất, Pending Context đã được dọn sạch.")
            
            
            # TEST 2: MULTI-BATCH (5-5-2)
            
            _cleanup()
            save_checkpoint_atomic(test_checkpoint_path, {"version": 1, "table_name": "orders", "watermark": initial_lower_wm}, "orders")
            
            print("\n[TEST 2] Chạy Multi-Batch Runner (Batch_size = 5, Max 12 records)...")
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
            print("  [PASS] Multi-Batch (5-5-2) hoàn tất.")

            
            # TEST 3: CRASH SAU KHI GHI BRONZE, TRƯỚC CHECKPOINT
            
            _cleanup()
            save_checkpoint_atomic(test_checkpoint_path, {"version": 1, "table_name": "orders", "watermark": initial_lower_wm}, "orders")
            
            print("\n[TEST 3] Giả lập Crash sau Bronze, trước Checkpoint...")
            with patch("__main__.save_checkpoint_atomic", side_effect=RuntimeError("Giả lập mất điện")):
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
                    assert "Giả lập mất điện" in str(e)
            
            # Assertions sau Crash
            saved_ckpt = load_checkpoint(test_checkpoint_path, "orders")
            assert saved_ckpt["watermark"] == initial_lower_wm, "Checkpoint không được phép thay đổi."
            assert test_pending_context_path.exists(), "Pending Context phải được bảo toàn."
            assert len(list(test_bronze_root.rglob("*.parquet"))) == 1, "File Bronze của Batch 1 phải tồn tại."
            print("  [PASS] Hệ thống bắt lỗi thành công, Checkpoint giữ nguyên vị trí, Pending Context còn tồn tại.")

            print("  [TEST 3] Bắt đầu chạy lại (Resume) để phục hồi...")
            recovery_result = run_orders_incremental_ingestion(
                conn=conn,
                bronze_root=test_bronze_root,
                checkpoint_path=test_checkpoint_path,
                pending_context_path=test_pending_context_path,
                batch_size=5,
                run_id="test_crash_1",
                run_started_at=datetime.now(),
                run_upper_watermark=test_upper
            )
            
            assert recovery_result["records_written"] == 12
            assert recovery_result["batches_committed"] == 3
            assert not test_pending_context_path.exists()
            print("  [PASS] Phục hồi (Resume) và chạy tiếp hoàn hảo.")

            
            # TEST 4: CRASH SAU CHECKPOINT, TRƯỚC KHI XÓA PENDING
            _cleanup()
            save_checkpoint_atomic(test_checkpoint_path, {"version": 1, "table_name": "orders", "watermark": initial_lower_wm}, "orders")
            
            print("\n[TEST 4] Giả lập Crash sau Checkpoint, trước khi xóa Pending...")
            with patch("__main__.delete_pending_batch_context", side_effect=RuntimeError("Giả lập tắt nguồn")):
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
                    assert "Giả lập tắt nguồn" in str(e)

            # Assertions sau Crash
            saved_ckpt = load_checkpoint(test_checkpoint_path, "orders")
            assert saved_ckpt["watermark"] != initial_lower_wm, "Checkpoint bắt buộc phải tiến lên."
            assert test_pending_context_path.exists(), "Pending Context phải rò rỉ ra ngoài do Crash."
            print("  [PASS] Hệ thống dính Crash, Checkpoint đã tiến lên, Pending Context thành rác.")

            print("  [TEST 4] Bắt đầu chạy lại để dọn rác và hoàn tất tiến trình...")
            recovery_result_2 = run_orders_incremental_ingestion(
                conn=conn,
                bronze_root=test_bronze_root,
                checkpoint_path=test_checkpoint_path,
                pending_context_path=test_pending_context_path,
                batch_size=5,
                run_id="test_crash_2",
                run_started_at=datetime.now(),
                run_upper_watermark=test_upper
            )
            
            # Batch 1 (5 records) đã commit, chỉ chạy tiếp 5 và 2 = 7 records, 2 batches.
            assert recovery_result_2["records_written"] == 7
            assert recovery_result_2["batches_committed"] == 2
            assert not test_pending_context_path.exists()
            print("  [PASS] Quét sạch Pending rác và hoàn thành 2 Batch cuối hoàn hảo.")

    finally:
        _cleanup()
        print("\nHoàn tất dọn dẹp file test.")