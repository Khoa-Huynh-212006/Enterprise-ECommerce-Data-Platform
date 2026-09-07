select
    order_id,
    customer_id,
    order_warehouse_id,
    order_status,

    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,

    to_char(order_purchase_timestamp::date, 'YYYYMMDD')::int
        as purchase_date_key,

    to_char(order_approved_at::date, 'YYYYMMDD')::int
        as approved_date_key,

    to_char(order_delivered_carrier_date::date, 'YYYYMMDD')::int
        as carrier_date_key,

    to_char(order_delivered_customer_date::date, 'YYYYMMDD')::int
        as delivered_date_key,

    to_char(order_estimated_delivery_date::date, 'YYYYMMDD')::int
        as estimated_delivery_date_key,

    item_line_count,
    total_quantity,
    distinct_product_count,
    distinct_seller_count,
    distinct_warehouse_count,
    merchandise_value,
    total_freight_value,

    payment_count,
    total_payment_value,
    max_payment_installments,
    payment_type_count,

    has_items,
    has_payment,
    has_order_warehouse,
    is_delivered,
    delivery_days,

    1 as order_count,

    source_system,
    created_at,
    updated_at
from {{ ref('int_orders_enriched') }}