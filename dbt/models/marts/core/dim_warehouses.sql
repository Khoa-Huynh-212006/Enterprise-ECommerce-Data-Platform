select
    warehouse_id,
    warehouse_city as city,
    warehouse_region as region,
    created_at,
    updated_at
from {{ source('staging_operational', 'warehouses') }}