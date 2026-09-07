with items as (
    select
        order_id, order_item_id, product_id, seller_id, warehouse_id,
        shipping_limit_date, quantity, price, freight_value,
        created_at, updated_at
    from {{ source('staging_operational', 'order_items') }}
),

orders as (
    select
        order_id, customer_id, order_status, order_purchase_timestamp
    from {{ ref('int_orders_enriched') }}
)

select
    i.order_id,
    i.order_item_id,
    o.customer_id,
    i.product_id,
    i.seller_id,
    i.warehouse_id,

    o.order_status,
    o.order_purchase_timestamp,
    i.shipping_limit_date,

    i.quantity,
    i.price as unit_price,
    i.freight_value,
    i.price * i.quantity as merchandise_value,
    i.price * i.quantity + i.freight_value as item_total_value,

    1 as item_line_count,

    i.created_at,
    i.updated_at

from items i
inner join orders o using (order_id)