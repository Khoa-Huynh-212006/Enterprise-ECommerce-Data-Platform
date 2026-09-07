select order_id, payment_sequential
from {{ source('staging_operational', 'order_payments') }}
group by order_id, payment_sequential
having count(*) > 1