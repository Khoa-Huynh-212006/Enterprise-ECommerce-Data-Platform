with orders as (
    select
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
    from {{ source('staging_operational', 'orders') }}
),

items as (
    select * from {{ ref('int_order_items_agg') }}
),

payments as (
    select * from {{ ref('int_order_payments_agg') }}
),

enriched as (
    select
        o.order_id,
        o.customer_id,

        -- Giữ warehouse gốc nhưng không coi đây là thuộc tính bắt buộc.
        o.warehouse_id as order_warehouse_id,

        o.order_status,
        o.order_purchase_timestamp,
        o.order_approved_at,
        o.order_delivered_carrier_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,

        i.item_line_count,
        i.total_quantity,
        i.distinct_product_count,
        i.distinct_seller_count,
        i.distinct_warehouse_count,
        i.merchandise_value,
        i.total_freight_value,

        p.payment_count,
        p.total_payment_value,
        p.max_payment_installments,
        p.payment_type_count,

        (i.order_id is not null) as has_items,
        (p.order_id is not null) as has_payment,
        (o.warehouse_id is not null) as has_order_warehouse,

        (o.order_delivered_customer_date is not null) as is_delivered,

        case
            when o.order_delivered_customer_date is not null
            then o.order_delivered_customer_date::date
                 - o.order_purchase_timestamp::date
        end as delivery_days,

        o.source_system,
        o.created_at,
        o.updated_at

    from orders o
    left join items i using (order_id)
    left join payments p using (order_id)
)

select *
from enriched