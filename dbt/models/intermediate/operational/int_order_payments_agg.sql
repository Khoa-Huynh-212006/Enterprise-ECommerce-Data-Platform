with payments as (
    select
        order_id,
        payment_type,
        payment_installments,
        payment_value
    from {{ source('staging_operational', 'order_payments') }}
),

aggregated as (
    select
        order_id,
        count(*) as payment_count,
        sum(payment_value) as total_payment_value,
        max(payment_installments) as max_payment_installments,
        count(distinct payment_type) as payment_type_count
    from payments
    group by order_id
)

select *
from aggregated