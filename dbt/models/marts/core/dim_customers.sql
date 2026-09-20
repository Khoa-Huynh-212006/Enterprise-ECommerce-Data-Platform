select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix as zip_code_prefix,
    customer_city as city,
    customer_state as state_code,
    created_at,
    updated_at
from {{ source('staging_operational', 'customers') }}