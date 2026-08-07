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
    delete_pending_batch_context
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

    # 2. Xây dựng và Lưu Pending Context (Ghi nhận trạng thái "Đang làm dở")
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
    
    # 5. Xóa Pending Context (Hoàn tất giao dịch)
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

    print("Bắt đầu Smoke Test: Single Batch + Pending Context (Happy Path)\n" + "-"*50)

    test_bronze_root = Path("test_bronze_pending")
    test_checkpoint_path = Path("test_pending_checkpoint.json")
    test_pending_context_path = Path("test_pending_context.json")

    # Dọn dẹp môi trường test
    for p in [test_bronze_root]:
        if p.exists(): shutil.rmtree(p)
    for p in [test_checkpoint_path, test_pending_context_path, Path(f"{test_pending_context_path}.tmp")]:
        if p.exists(): p.unlink()

    engine = get_engine()

    try:
        with engine.connect() as conn:
            # Lấy mốc Upper từ DB
            upper_wm = get_upper_watermark(conn)
            if not upper_wm:
                raise RuntimeError("Bảng rỗng, không đủ dữ liệu để test!")
                
            initial_lower_wm = {"updated_at": "1970-01-01T00:00:00.000000", "order_id": ""}
            
            print("Chạy thử 1 Batch 5 records với Pending Context Guard...")
            
            result = process_one_orders_batch(
                conn=conn,
                lower_watermark=initial_lower_wm,
                run_upper_watermark=upper_wm,
                batch_size=5,
                bronze_root=test_bronze_root,
                checkpoint_path=test_checkpoint_path,
                pending_context_path=test_pending_context_path,
                run_id="test_run_pending_001",
                extraction_id="test_run_pending_001_batch_001",
                ingested_at=datetime.now()
            )

            # 1. Hợp đồng trả về
            assert result["status"] == "batch_committed", "Sai status trả về."
            assert result["checkpoint_updated"] is True, "Checkpoint báo chưa update."
            assert result["pending_context_deleted"] is True, "Pending Context báo chưa xóa."
            assert result["output_path"].exists(), "File Parquet không tồn tại."
            print("   Assert 1: Return contract trả về chuẩn xác. File Bronze đã được tạo.")

            # 2. Checkpoint đồng bộ Record cuối
            written_df = pd.read_parquet(result["output_path"])
            last_row = written_df.iloc[-1]
            last_row_updated_at_str = last_row["updated_at"].strftime("%Y-%m-%dT%H:%M:%S.%f")
            
            saved_checkpoint = load_checkpoint(test_checkpoint_path, "orders")
            assert saved_checkpoint["watermark"] == {
                "updated_at": last_row_updated_at_str,
                "order_id": last_row["order_id"]
            }, "LỖI: Checkpoint không khớp dòng cuối Parquet!"
            print("   Assert 2: Checkpoint commit đúng mốc.")

            # 3. Quản trị Pending Context
            assert not test_pending_context_path.exists(), "File Pending Context vẫn còn tồn tại sau khi Commit!"
            assert not Path(f"{test_pending_context_path}.tmp").exists(), "File .tmp của Pending Context bị rò rỉ!"
            print("   Assert 3: Pending Context và file .tmp rác đã bị xóa sổ hoàn toàn sau khi thành công.")

    finally:
        for p in [test_bronze_root]:
            if p.exists(): shutil.rmtree(p)
        for p in [test_checkpoint_path, test_pending_context_path, Path(f"{test_pending_context_path}.tmp")]:
            if p.exists(): p.unlink()
        print("\nHoàn tất dọn dẹp file test.")