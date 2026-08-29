from sqlalchemy import Connection

from fastorder.ingestion.incremental.table_config import (
    ORDERS_CONFIG,
)

from fastorder.ingestion.incremental.table_extractor import (
    get_upper_watermark as get_table_upper_watermark,
    extract_table_batch,
    _format_timestamp,
)

def get_upper_watermark(
    conn: Connection,
):
    return get_table_upper_watermark(
        conn=conn,
        config=ORDERS_CONFIG,
    )


def extract_orders_batch(
    conn: Connection,
    lower_watermark,
    upper_watermark,
    batch_size: int = 1000,
):
    return extract_table_batch(
        conn=conn,
        config=ORDERS_CONFIG,
        lower_watermark=lower_watermark,
        upper_watermark=upper_watermark,
        batch_size=batch_size,
    )

if __name__ == "__main__":
    from fastorder.db.connection import get_engine
    
    print("Bắt đầu Smoke Test: Orders Extractor\n" + "-"*50)
    
    engine = get_engine()
    
    with engine.connect() as connection:
        # 1. Chụp Upper Watermark
        upper_wm = get_upper_watermark(connection)
        print(f"[1] Upper Bound ghi nhận: {upper_wm}")
        
        if not upper_wm:
            print("Bảng orders trống, không có dữ liệu để test.")
        else:
            # 2. Test quét Batch 1
            initial_lower_wm = {
                "updated_at": "1970-01-01T00:00:00.000000",
                "order_id": ""
            }
            
            batch_1_records, batch_1_next_wm = extract_orders_batch(
                conn=connection,
                lower_watermark=initial_lower_wm,
                upper_watermark=upper_wm,
                batch_size=5
            )
            print(f"[2] Batch 1: Lấy thành công {len(batch_1_records)} records.")
            
            if batch_1_records:
                # Assertion 1: Watermark của batch 1 phải khớp với record cuối cùng
                expected_batch_1_watermark = {
                    "updated_at": _format_timestamp(batch_1_records[-1]["updated_at"]),
                    "order_id": batch_1_records[-1]["order_id"]
                }
                assert batch_1_next_wm == expected_batch_1_watermark, "Watermark trả về sai với record cuối."
                print("    -> Assertion (Watermark khớp record cuối) Passed.")

                # 3. Test quét Batch 2
                if len(batch_1_records) == 5:
                    batch_2_records, batch_2_next_wm = extract_orders_batch(
                        conn=connection,
                        lower_watermark=batch_1_next_wm,
                        upper_watermark=upper_wm,
                        batch_size=5
                    )
                    print(f"[3] Batch 2: Lấy thành công {len(batch_2_records)} records.")
                    
                    if batch_2_records:
                        # Chuẩn bị Data cho Assertion 2 & 3
                        batch_1_positions = {
                            (_format_timestamp(r["updated_at"]), r["order_id"]) for r in batch_1_records
                        }
                        batch_2_positions = {
                            (_format_timestamp(r["updated_at"]), r["order_id"]) for r in batch_2_records
                        }
                        
                        # Assertion 2: Đảm bảo không trùng lặp (Disjoint)
                        assert batch_1_positions.isdisjoint(batch_2_positions), "Trùng lặp dữ liệu giữa 2 Batch."
                        print("    -> Assertion (Disjoint Batches) Passed.")
                        
                        # Assertion 3: Đảm bảo nối tiếp tăng dần (Monotonically Increasing)
                        last_batch_1_position = (batch_1_records[-1]["updated_at"], batch_1_records[-1]["order_id"])
                        first_batch_2_position = (batch_2_records[0]["updated_at"], batch_2_records[0]["order_id"])
                        assert first_batch_2_position > last_batch_1_position, "Batch 2 không tịnh tiến tăng dần so với Batch 1."
                        print("    -> Assertion (Monotonically Increasing) Passed.")
                else:
                    print("\nKhông đủ 5 records để test tính phân trang của Batch #2.")