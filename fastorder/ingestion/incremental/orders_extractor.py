from sqlalchemy import Connection, text
from typing import List, Dict, Tuple, Optional
from datetime import datetime

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"

def _format_timestamp(dt: datetime) -> str:
    """
    Hàm hỗ trợ: Định dạng đối tượng datetime của PostgreSQL 
    thành chuỗi ISO 8601 naive (không timezone) theo đúng Data Contract.
    """
    return dt.strftime(TIMESTAMP_FORMAT)

def _parse_timestamp(value: str) -> datetime:
    """
    Hàm hỗ trợ: Parse ngược chuỗi Watermark thành datetime object
    để truyền trực tiếp vào SQLAlchemy an toàn.
    """
    try:
        return datetime.strptime(value, TIMESTAMP_FORMAT)
    except ValueError as error:
        raise ValueError(
            f"Timestamp không hợp lệ: '{value}'. "
            "Kỳ vọng YYYY-MM-DDTHH:MM:SS.ffffff."
        ) from error

def get_upper_watermark(conn: Connection) -> Optional[Dict[str, str]]:
    """
    Chụp lại Watermark lớn nhất (mới nhất) tại thời điểm gọi hàm.
    Đóng băng biên chạy cho một vòng lặp Extraction.
    """
    query = text("""
        SELECT updated_at, order_id
        FROM orders
        ORDER BY updated_at DESC, order_id DESC
        LIMIT 1;
    """)
    
    result = conn.execute(query).fetchone()
    
    if not result:
        return None
        
    return {
        "updated_at": _format_timestamp(result.updated_at),
        "order_id": result.order_id
    }

def extract_orders_batch(
    conn: Connection,
    lower_watermark: Dict[str, str],
    upper_watermark: Dict[str, str],
    batch_size: int = 1000
) -> Tuple[List[Dict], Dict[str, str]]:
    """
    Trích xuất một batch dữ liệu nằm trong khoảng (lower_watermark, upper_watermark].
    """
    # 1. Validate Input Params
    if not isinstance(batch_size, int) or isinstance(batch_size, bool):
        raise ValueError("batch_size phải là một số nguyên.")
    if batch_size <= 0:
        raise ValueError("batch_size phải lớn hơn 0.")

    # 2. Parse & So sánh Watermark Logic
    lower_position = (
        _parse_timestamp(lower_watermark["updated_at"]),
        lower_watermark["order_id"]
    )
    upper_position = (
        _parse_timestamp(upper_watermark["updated_at"]),
        upper_watermark["order_id"]
    )

    if lower_position > upper_position:
        raise ValueError("Lower watermark không được lớn hơn upper watermark.")
    if lower_position == upper_position:
        return [], lower_watermark

    # 3. Thực thi Query (Explicit Schema Contract)
    query = text("""
        SELECT
            order_id,
            customer_id,
            warehouse_id,
            order_status,
            order_purchase_timestamp,
            order_approved_at,
            order_delivered_carrier_date,
            order_delivered_customer_date,
            order_estimated_delivery_date,
            source_system,
            created_at,
            updated_at
        FROM orders
        WHERE (
            updated_at > :lower_updated_at
            OR (
                updated_at = :lower_updated_at
                AND order_id > :lower_order_id
            )
        )
        AND (
            updated_at < :upper_updated_at
            OR (
                updated_at = :upper_updated_at
                AND order_id <= :upper_order_id
            )
        )
        ORDER BY updated_at ASC, order_id ASC
        LIMIT :batch_size;
    """)
    
    params = {
        "lower_updated_at": lower_position[0],
        "lower_order_id": lower_position[1],
        "upper_updated_at": upper_position[0],
        "upper_order_id": upper_position[1],
        "batch_size": batch_size
    }
    
    results = conn.execute(query, params).fetchall()
    
    if not results:
        return [], lower_watermark
        
    records = [dict(row._mapping) for row in results]
    
    last_record = results[-1]
    next_lower_watermark = {
        "updated_at": _format_timestamp(last_record.updated_at),
        "order_id": last_record.order_id
    }
    
    return records, next_lower_watermark


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