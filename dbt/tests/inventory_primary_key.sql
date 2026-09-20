select warehouse_id, product_id
from {{ source('staging_operational', 'inventory') }}
group by warehouse_id, product_id
having count(*) > 1