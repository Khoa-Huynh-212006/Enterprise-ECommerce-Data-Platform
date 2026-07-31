import random
from sqlalchemy import text
from fastorder.db.connection import get_engine

TERMINAL_STATUSES = {
    "delivered",
    "canceled",
    "unavailable"
}

STATUS_TRANSITIONS = {
    "created": [
        {"next_status": "approved", "weight": 90},
        {"next_status": "canceled", "weight": 8},
        {"next_status": "unavailable", "weight": 2}
    ],
    "approved": [
        {"next_status": "processing", "weight": 97},
        {"next_status": "canceled", "weight": 3}
    ],
    "processing": [
        {"next_status": "shipped", "weight": 98},
        {"next_status": "canceled", "weight": 2}
    ],
    "shipped": [
        {"next_status": "delivered", "weight": 100}
    ]
}

def get_next_status(current_status: str) -> str | None:
    if current_status in TERMINAL_STATUSES:
        return None

    if current_status not in STATUS_TRANSITIONS:
        raise ValueError(f"Order status không hợp lệ: {current_status}")

    transitions = STATUS_TRANSITIONS[current_status]

    possible_statuses = [
        transition["next_status"]
        for transition in transitions
    ]
    weights = [
        transition["weight"]
        for transition in transitions
    ]

    return random.choices(
        possible_statuses,
        weights=weights,
        k=1
    )[0]

def test_fetch_and_transition():
    engine = get_engine()
    
    with engine.connect() as conn:
        # Lấy 1 order có trạng thái hợp lệ để chuyển tiếp
        query = text("""
            SELECT order_id, order_status 
            FROM orders 
            WHERE order_status NOT IN ('delivered', 'canceled', 'unavailable')
            LIMIT 1;
        """)
        
        result = conn.execute(query).fetchone()
        
        if not result:
            print("Không tìm thấy order nào cần chuyển trạng thái.")
            print("Gợi ý: Hãy chạy script sinh đơn hàng CREATE_ORDER trước.")
            return
            
        order_id, current_status = result
        
        print(f"Order ID       : {order_id}")
        print(f"Current Status : {current_status}")
        
        next_status = get_next_status(current_status)
        print(f"Next Status    : {next_status}")

if __name__ == "__main__":
    print("--- KIỂM TRA LỖI STATUS KHÔNG HỢP LỆ ---")
    try:
        get_next_status("unknown_status_123")
    except ValueError as error:
        print(f"Bắt được lỗi: {error}")
        
    print("\n--- KIỂM TRA ĐỌC DATABASE & MÔ PHỎNG TRANSITION ---")
    test_fetch_and_transition()