import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, Any
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
            "checkpoint_updated": False
        }

    # 2. Ghi Bronze Parquet (Tạo output_path trỏ tới file part-000.parquet)
    output_path = write_bronze_batch(
        records=batch_records,
        bronze_root=bronze_root,
        table_name="orders",
        extraction_id=extraction_id,
        ingested_at=ingested_at
    )

    # 3. Ghi Checkpoint
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

    # 4. Trả về đúng File Path thay vì Root Path
    return {
        "status": "batch_committed",
        "records_written": len(batch_records),
        "output_path": output_path,
        "next_watermark": next_watermark
    }


if __name__ == "__main__":
    import shutil
    from fastorder.db.connection import get_engine
    from fastorder.ingestion.incremental.orders_extractor import get_upper_watermark

    print("Bắt đầu Smoke Test: Incremental Runner (Single Batch)\n")

    test_bronze_root = Path("test_bronze_data")
    test_checkpoint_path = Path("test_runner_checkpoint.json")

    # Dọn dẹp môi trường test cũ nếu có
    if test_bronze_root.exists(): shutil.rmtree(test_bronze_root)
    if test_checkpoint_path.exists(): test_checkpoint_path.unlink()

    engine = get_engine()

    try: 
        with engine.connect() as conn:
            initial_checkpoint = load_checkpoint(test_checkpoint_path, "orders")
            lower_wm = initial_checkpoint["watermark"]
            upper_wm = get_upper_watermark(conn)

            if not upper_wm:
                raise RuntimeError("Smoke test thất bại: bảng orders không có dữ liệu.")

            print(f"-> Lower WM ban đầu: {lower_wm}")
            print(f"-> Upper WM ghi nhận: {upper_wm}")

            print("\nĐang xử lý 1 batch 5 records...")
            result = process_one_orders_batch(
                conn=conn,
                lower_watermark=lower_wm,
                run_upper_watermark=upper_wm,
                batch_size=5,
                bronze_root=test_bronze_root,
                checkpoint_path=test_checkpoint_path,
                extraction_id="run_batch_001",
                ingested_at=datetime.now() # Đã sửa thành truyền trực tiếp đối tượng datetime
            )

            if result["status"] == "completed":
                print("Không có record nào để xử lý. Batch đã hoàn tất.")
            else:
                print("Xử lý thành công!")
                
                # Invariant 1: Checkpoint đã thực sự được lưu xuống đĩa chưa?
                saved_checkpoint = load_checkpoint(test_checkpoint_path, "orders")
                assert saved_checkpoint["watermark"] == result["next_watermark"], \
                    "LỖI: Checkpoint lưu dưới đĩa không khớp với kết quả trả về!"
                print("   Assert 1: Checkpoint dưới đĩa khớp với Next Watermark.")

                # Invariant 2: Watermark có trỏ đúng vào dòng cuối cùng của Parquet không?
                written_df = pd.read_parquet(result["output_path"])
                last_row = written_df.iloc[-1]
                
                # Format datetime của Pandas về định dạng ISO 8601 Naive
                last_row_updated_at_str = last_row["updated_at"].strftime("%Y-%m-%dT%H:%M:%S.%f")
                
                assert saved_checkpoint["watermark"] == {
                    "updated_at": last_row_updated_at_str,
                    "order_id": last_row["order_id"]
                }, "LỖI: Checkpoint không khớp với dòng cuối cùng của file Parquet!"
                print("   Assert 2: Checkpoint trỏ chính xác vào dòng cuối của file Parquet.")
                
    finally:
        if test_bronze_root.exists(): shutil.rmtree(test_bronze_root)
        if test_checkpoint_path.exists(): test_checkpoint_path.unlink()
        print("\nHoàn tất dọn dẹp file test.")