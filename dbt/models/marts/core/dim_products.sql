with products as (
    select *
    from {{ source('staging_operational', 'products') }}
),

translations as (
    select *
    from {{ source('staging_operational', 'product_category_name_translation') }}
)

select
    p.product_id,
    p.product_category_name as category_name,
    t.product_category_name_english as category_name_english,
    p.product_name_lenght as product_name_length,
    p.product_description_lenght as product_description_length,
    p.product_photos_qty as product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    p.created_at,
    p.updated_at
from products p
left join translations t
    on p.product_category_name = t.product_category_name