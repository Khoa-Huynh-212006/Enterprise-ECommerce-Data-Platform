select order_id, order_item_id
from {{ ref('fact_order_items') }}
group by order_id, order_item_id
having count(*) > 1