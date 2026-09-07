select order_id, order_item_id
from {{ source('staging_operational', 'order_items') }}
group by order_id, order_item_id
having count(*) > 1