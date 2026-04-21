-- models/marts/dim_customer.sql

with customer as (
    select *
    from {{ ref('int_customer_enriched') }}
)

select
    customer_id,
    customer_full_name,
    email,
    active_status,
    city,
    district,
    address,
    postal_code,
    customer_since
from customer