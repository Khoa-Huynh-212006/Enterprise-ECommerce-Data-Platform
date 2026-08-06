from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional
from sqlalchemy import Connection

from fastorder.ingestion.incremental.orders_extractor import extract_orders_batch
from fastorder.ingestion.incremental.bronze_writer import write_bronze_batch
from fastorder.ingestion.incremental.checkpoint_manager import save_checkpoint_atomic, load_checkpoint

def process_one_orders_batch(
        conn: Connection,
        lower_watermark: Dict[str, str],
        run_upper_watermark: Dict[str, str],
        batch_size: int, 
        bronze_root: Path, 
        checkpoint_path: Path,
        extraction_id: str,
        ingested_at: datetime
) -> Dict[str, Any]:
    
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
            "checkpoint_updated": False
        }

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

    return {
        "status": "batch_committed",
        "records_written": len(batch_records),
        "output_path": output_path,
        "next_watermark": next_watermark,
        "checkpoint_updated": True
    }


def run_orders_incremental_ingestion(
    conn: Connection,
    bronze_root: Path,
    checkpoint_path: Path,
    batch_size: int,
    run_id: str,
    run_started_at: datetime,
    run_upper_watermark: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    
    # 1. Validate Input (Chặn bool vì isinstance(True, int) == True)
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

    # 2. Khởi tạo mốc chạy
    checkpoint = load_checkpoint(checkpoint_path, "orders")
    current_lower = checkpoint["watermark"]

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

    if curr_key == upper_key:
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

    batch_number = 1
    total_records = 0
    output_paths = []

    # 4. Vòng lặp Multi-batch
    while curr_key < upper_key:
        extraction_id = f"{run_id}_batch_{batch_number:06d}"
        
        result = process_one_orders_batch(
            conn=conn,
            lower_watermark=current_lower,
            run_upper_watermark=run_upper_watermark,
            batch_size=batch_size,
            bronze_root=bronze_root,
            checkpoint_path=checkpoint_path,
            extraction_id=extraction_id,
            ingested_at=run_started_at
        )

        # Chặn lỗi Logic: Chưa chạm Upper mà đã hết dữ liệu
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
    from fastorder.db.connection import get_engine
    from fastorder.ingestion.incremental.orders_extractor import get_upper_watermark

    print("Bắt đầu Smoke Test: Multi-Batch Runner (12 Records)\n" + "-"*50)

    test_bronze_root = Path("test_bronze_multibatch")
    test_checkpoint_path = Path("test_multi_checkpoint.json")

    if test_bronze_root.exists(): shutil.rmtree(test_bronze_root)
    if test_checkpoint_path.exists(): test_checkpoint_path.unlink()

    engine = get_engine()

    try:
        with engine.connect() as conn:
            full_upper = get_upper_watermark(conn)
            if not full_upper:
                raise RuntimeError("Bảng rỗng, không đủ dữ liệu để test!")
                
            initial_lower_wm = {"updated_at": "1970-01-01T00:00:00.000000", "order_id": ""}
            
            twelve_records, test_upper = extract_orders_batch(
                conn=conn,
                lower_watermark=initial_lower_wm,
                upper_watermark=full_upper,
                batch_size=12
            )
            
            # Kiểm tra gắt gao: Bắt buộc phải có đúng 12 records
            if len(twelve_records) != 12:
                raise RuntimeError(
                    f"Smoke test cần đúng 12 records, "
                    f"nhưng extractor chỉ trả về {len(twelve_records)}."
                )
            
            print(f"-> Tạo thành công biên ảo tại record thứ 12: {test_upper}")

            print("\nChạy Incremental Runner (Batch_size = 5, Max 12 records)...")
            run_result = run_orders_incremental_ingestion(
                conn=conn,
                bronze_root=test_bronze_root,
                checkpoint_path=test_checkpoint_path,
                batch_size=5,
                run_id="test_multi_run_2026",
                run_started_at=datetime.now(),
                run_upper_watermark=test_upper
            )

            print(f"\nSummary: {run_result['status']} | Batches: {run_result['batches_committed']} | Rows: {run_result['records_written']}")
            
            # Assert 1: Thống kê cơ bản
            assert run_result["status"] == "run_completed", "Trạng thái run bị sai."
            assert run_result["records_written"] == 12, "Tổng số records không bằng 12."
            assert run_result["batches_committed"] == 3, "Phải chia thành 3 batches."
            assert len(run_result["output_paths"]) == 3, "Phải có 3 file output."
            print("   Assert 1: Thống kê cơ bản chuẩn xác.")

            # Assert 2: Cấu trúc 5-5-2
            batch_row_counts = [len(pd.read_parquet(path)) for path in run_result["output_paths"]]
            assert batch_row_counts == [5, 5, 2], f"Kích thước batch không đúng: {batch_row_counts}"
            print("   Assert 2: Kích thước từng batch chia đúng tỷ lệ 5-5-2.")

            # Assert 3: Chống trùng / Mất record bằng Order ID
            output_order_ids = []
            for path in run_result["output_paths"]:
                batch_df = pd.read_parquet(path)
                output_order_ids.extend(batch_df["order_id"].tolist())

            expected_order_ids = [record["order_id"] for record in twelve_records]
            assert output_order_ids == expected_order_ids, "Dữ liệu nhiều batch bị mất, trùng hoặc sai thứ tự."
            print("   Assert 3: Trình tự Order ID bảo toàn tuyệt đối xuyên suốt các file Parquet.")

            # Assert 4: Checkpoint
            saved_checkpoint = load_checkpoint(test_checkpoint_path, "orders")
            assert saved_checkpoint["watermark"] == test_upper, "Checkpoint đĩa không khớp Test Upper."
            assert run_result["final_watermark"] == test_upper, "Final Watermark bộ nhớ không khớp Test Upper."
            print("   Assert 4: Checkpoint cuối chạm đúng Upper Watermark.")
            
            # Assert 5: File rác
            assert not list(test_bronze_root.rglob("*.tmp")), "File rác .tmp bị rò rỉ!"
            print("   Assert 5: Tuyệt đối không rò rỉ file .tmp.")
            
    finally:
        if test_bronze_root.exists(): 
            shutil.rmtree(test_bronze_root)
        if test_checkpoint_path.exists(): 
            test_checkpoint_path.unlink()
        print("\nHoàn tất dọn dẹp file test.")