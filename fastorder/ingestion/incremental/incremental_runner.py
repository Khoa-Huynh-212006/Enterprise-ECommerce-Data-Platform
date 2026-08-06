from pathlib import Path
from datetime import datetime
from typing import Dict, Any
from sqlalchemy import Connection

from fastorder.ingestion.incremental.orders_extractor import extract_orders_batch
from fastorder.ingestion.incremental.bronze_writer import write_bronze_batch
from fastorder.ingestion.incremental.checkpoint_manager import save_checkpoint_atomic

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

    # 2. Ghi Bronze Parquet
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

    # 4. Trả về hợp đồng chuẩn xác
    return {
        "status": "batch_committed",
        "records_written": len(batch_records),
        "output_path": output_path,
        "next_watermark": next_watermark,
        "checkpoint_updated": True
    }

if __name__ == "__main__":
    import shutil
    import pandas as pd
    from unittest.mock import patch
    from fastorder.db.connection import get_engine
    from fastorder.ingestion.incremental.orders_extractor import get_upper_watermark
    from fastorder.ingestion.incremental.checkpoint_manager import load_checkpoint

    test_bronze_root = Path("test_bronze_data")
    test_checkpoint_path = Path("test_runner_checkpoint.json")

    if test_bronze_root.exists(): 
        shutil.rmtree(test_bronze_root)
    if test_checkpoint_path.exists(): 
        test_checkpoint_path.unlink()

    engine = get_engine()

    try: 
        with engine.connect() as conn:
            upper_wm = get_upper_watermark(conn)
            if not upper_wm:
                raise RuntimeError("Smoke test thất bại: bảng orders không có dữ liệu.")

            # TEST 1: HAPPY PATH
            print("Bắt đầu Smoke Test 1: Incremental Runner (Happy Path)\n" + "-"*50)
            
            initial_checkpoint = load_checkpoint(test_checkpoint_path, "orders")
            lower_wm = initial_checkpoint["watermark"]

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
                extraction_id="run_batch_happy",
                ingested_at=datetime.now()
            )

            if result["status"] == "completed":
                print("Không có record nào để xử lý. Batch đã hoàn tất.")
            else:
                print("Xử lý thành công!")
                # Invariant 1
                saved_checkpoint = load_checkpoint(test_checkpoint_path, "orders")
                assert saved_checkpoint["watermark"] == result["next_watermark"], \
                    "LỖI: Checkpoint lưu dưới đĩa không khớp với kết quả trả về!"
                print("   Assert 1: Checkpoint dưới đĩa khớp với Next Watermark.")

                # Invariant 2
                written_df = pd.read_parquet(result["output_path"])
                last_row = written_df.iloc[-1]
                last_row_updated_at_str = last_row["updated_at"].strftime("%Y-%m-%dT%H:%M:%S.%f")
                assert saved_checkpoint["watermark"] == {
                    "updated_at": last_row_updated_at_str,
                    "order_id": last_row["order_id"]
                }, "LỖI: Checkpoint không khớp với dòng cuối của file Parquet!"
                print("   Assert 2: Checkpoint trỏ chính xác vào dòng cuối của file Parquet.")

            # TEST 2: FAILURE ORDERING (Giả lập Lỗi Writer)
            print("\nBắt đầu Smoke Test 2: Failure Ordering\n" + "-"*50)
            
            # Reset lại toàn bộ môi trường để test lỗi một cách tinh khiết nhất
            if test_bronze_root.exists(): shutil.rmtree(test_bronze_root)
            if test_checkpoint_path.exists(): test_checkpoint_path.unlink()

            # Khởi tạo lại checkpoint gốc
            initial_fail_checkpoint = load_checkpoint(test_checkpoint_path, "orders")
            lower_wm_fail = initial_fail_checkpoint["watermark"]
            print(f"-> Lower WM ban đầu (trước khi lỗi): {lower_wm_fail}")

            # Dùng mock.patch để chặn họng hàm write_bronze_batch bên trong incremental_runner
            patcher = patch("__main__.write_bronze_batch")
            mock_writer = patcher.start()
            # Ép hàm này quăng lỗi khi bị gọi
            mock_writer.side_effect = RuntimeError("Simulated Bronze failure")

            print("Đang xử lý 1 batch nhưng Writer sẽ bị ném lỗi giữa chừng...")
            error_caught = False
            try:
                process_one_orders_batch(
                    conn=conn,
                    lower_watermark=lower_wm_fail,
                    run_upper_watermark=upper_wm,
                    batch_size=5,
                    bronze_root=test_bronze_root,
                    checkpoint_path=test_checkpoint_path,
                    extraction_id="run_batch_fail",
                    ingested_at=datetime.now()
                )
            except RuntimeError as e:
                error_caught = True
                assert str(e) == "Simulated Bronze failure", "Lỗi ném ra không đúng như giả lập!"
                print("   Assert 3: Lỗi đã được truyền ra ngoài (Bubbled up) thành công.")

            # Dừng mock
            patcher.stop()

            assert error_caught, "LỖI: Hàm không raise exception như kỳ vọng!"

            # Đảm bảo Checkpoint KHÔNG đổi
            checkpoint_after_failure = load_checkpoint(test_checkpoint_path, "orders")
            assert checkpoint_after_failure["watermark"] == lower_wm_fail, \
                "LỖI: Checkpoint bị tiến lên dù Writer thất bại! At-least-once semantics đã bị phá vỡ!"
            print("   Assert 4: Checkpoint được bảo toàn (Giữ nguyên mốc ban đầu).")

            # Đảm bảo không có rác (Parquet hay Tmp)
            parquet_files = list(test_bronze_root.rglob("*.parquet"))
            tmp_files = list(test_bronze_root.rglob("*.tmp"))
            assert not parquet_files, f"LỖI: File Parquet vẫn được tạo ra: {parquet_files}"
            assert not tmp_files, f"LỖI: File .tmp bị rò rỉ: {tmp_files}"
            print("   Assert 5: Không có bất kỳ file Parquet hay .tmp rác nào được sinh ra.")

    finally:
        if test_bronze_root.exists(): shutil.rmtree(test_bronze_root)
        if test_checkpoint_path.exists(): test_checkpoint_path.unlink()
        print("\nHoàn tất dọn dẹp file test.")