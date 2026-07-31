import random
import uuid
from datetime import datetime
from decimal import Decimal
from sqlalchemy import text, bindparam
from fastorder.db.connection import get_engine

def get_target_product_count() -> int: # Hàm chốt số loại sản phẩm sẽ mua
    """Xác định số lượng mặt hàng khác nhau trong 1 đơn hàng dựa trên trọng số"""
    r = random.random()
    if r < 0.40: 
        return 1
    elif r < 0.75: # 35% 
        return random.randint(2, 3)
    elif r < 0.95: # 20%
        return random.randint(4, 10)
    else:          # 5%
        return random.randint(11, 50)

def generate_single_order(): #hàm tạo đơn hàng
    engine = get_engine()
    
    with engine.begin() as conn:

        customers = conn.execute(text("SELECT customer_id FROM customers")).scalars().all()
        if not customers:
            raise ValueError("Prerequisite Failed: Bảng customers rỗng.")
            
        warehouses = conn.execute(text("SELECT warehouse_id FROM warehouses")).scalars().all()
        if not warehouses:
            raise ValueError("Prerequisite Failed: Bảng warehouses rỗng.")
            
        fallback_seller = conn.execute(text("SELECT seller_id FROM sellers LIMIT 1")).scalar() # chọn seller dự phòng
        if not fallback_seller:
            raise ValueError("Prerequisite Failed: Bảng sellers rỗng.")

        customer_id = random.choice(customers)
        warehouse_id = random.choice(warehouses)


        inventory_query = text("""
            SELECT product_id, quantity_available 
            FROM inventory 
            WHERE warehouse_id = :wh AND quantity_available > 0
        """)
        available_products = conn.execute(inventory_query, {"wh": warehouse_id}).all()
        
        if not available_products:
            print(f"Kho {warehouse_id} hiện tại không còn mặt hàng nào. Bỏ qua đơn hàng.")
            return None

        target_count = get_target_product_count()
        actual_count = min(target_count, len(available_products))
        
        selected_products = random.sample(available_products, actual_count)
        product_ids = [p[0] for p in selected_products]

        price_query = text("""
            SELECT DISTINCT ON (product_id)
                   product_id, price, freight_value, seller_id
            FROM order_items
            WHERE product_id IN :pids
            ORDER BY product_id, shipping_limit_date DESC NULLS LAST
        """)
        price_query = price_query.bindparams(bindparam("pids", expanding=True))
        historical_data = conn.execute(price_query, {"pids": product_ids}).all()
        
        price_dict = {row[0]: (row[1], row[2], row[3]) for row in historical_data}

        order_id = uuid.uuid4().hex[:32]
        total_payment = Decimal("0.00")
        total_quantity = 0
        order_items_data = []
        inventory_updates = []
        
        for idx, (p_id, qty_avail) in enumerate(selected_products):
            max_buy = min(10, qty_avail)
            buy_qty = random.randint(1, max_buy)
            
            hist_price, hist_freight, seller_id = price_dict.get(
                p_id, (Decimal("50.00"), Decimal("15.00"), None)
            )
            seller_id = seller_id or fallback_seller
                
            order_items_data.append({
                "order_id": order_id,
                "order_item_id": idx + 1,
                "product_id": p_id,
                "seller_id": seller_id,
                "price": hist_price,
                "freight_value": hist_freight,
                "quantity": buy_qty,
                "warehouse_id": warehouse_id
            })
            
            inventory_updates.append({
                "warehouse_id": warehouse_id,
                "product_id": p_id,
                "buy_qty": buy_qty
            })
            
            total_payment += (hist_price * buy_qty) + hist_freight
            total_quantity += buy_qty


        order_query = text("""
            INSERT INTO orders (order_id, customer_id, order_status, order_purchase_timestamp, warehouse_id)
            VALUES (:order_id, :customer_id, 'created', :created_at, :warehouse_id)
        """)
        conn.execute(order_query, {
            "order_id": order_id, "customer_id": customer_id, 
            "created_at": datetime.now(), "warehouse_id": warehouse_id
        })
        
        items_query = text("""
            INSERT INTO order_items (order_id, order_item_id, product_id, seller_id, price, freight_value, quantity, warehouse_id)
            VALUES (:order_id, :order_item_id, :product_id, :seller_id, :price, :freight_value, :quantity, :warehouse_id)
        """)
        conn.execute(items_query, order_items_data)
        
        payment_types = ['credit_card', 'debit_card', 'bank_transfer', 'e_wallet', 'cash_on_delivery']
        selected_payment_type = random.choice(payment_types)
        
        payment_query = text("""
            INSERT INTO order_payments (order_id, payment_sequential, payment_type, payment_installments, payment_value)
            VALUES (:order_id, 1, :payment_type, 1, :payment_value)
        """)
        conn.execute(payment_query, {
            "order_id": order_id,
            "payment_type": selected_payment_type,
            "payment_value": total_payment
        })
        
        update_inv_query = text("""
            UPDATE inventory 
            SET quantity_available = quantity_available - :buy_qty,
                updated_at = CURRENT_TIMESTAMP
            WHERE warehouse_id = :warehouse_id 
              AND product_id = :product_id
              AND quantity_available >= :buy_qty
        """)
        
        for inv_update in inventory_updates:
            res = conn.execute(update_inv_query, inv_update)
            if res.rowcount != 1:
                raise ValueError(
                    f"Overselling/Race condition detected for product {inv_update['product_id']}. "
                    "Transaction rollback."
                )

        return {
            "order_id": order_id,
            "warehouse_id": warehouse_id,
            "distinct_product_count": actual_count,
            "total_quantity": total_quantity,
            "total_payment": total_payment,
            "payment_type": selected_payment_type
        }

if __name__ == "__main__":
    print("Bắt đầu tạo 1 đơn hàng giả lập...")
    try:
        result = generate_single_order()
        if result:
            print("Đã tạo thành công đơn hàng:")
            print(f"   - Order ID: {result['order_id']}")
            print(f"   - Kho xử lý: {result['warehouse_id']}")
            print(f"   - Số loại SP: {result['distinct_product_count']} | Tổng SL: {result['total_quantity']}")
            print(f"   - Thanh toán: ${result['total_payment']:,.2f} ({result['payment_type']})")
    except Exception as e:
        print(f"Lỗi khi tạo đơn hàng: {e}")
        raise