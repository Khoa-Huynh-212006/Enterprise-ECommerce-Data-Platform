with order_items as (
    select
        order_id,
        product_id,
        seller_id,
        warehouse_id,
        quantity,
        price,
        freight_value
    from {{ source('staging_operational', 'order_items') }}
),

aggregated as (
    select
        order_id,
        count(*) as item_line_count,
        sum(quantity) as total_quantity,
        count(distinct product_id) as distinct_product_count,
        count(distinct seller_id) as distinct_seller_count,
        count(distinct warehouse_id) as distinct_warehouse_count,
        sum(price * quantity) as merchandise_value,
        sum(freight_value) as total_freight_value
    from order_items
    group by order_id
)

select *
from aggregated