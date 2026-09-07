select review_id, order_id
from {{ source('staging_operational', 'order_reviews') }}
group by review_id, order_id
having count(*) > 1