with customers as (
    select *
    from {{ ref('dim_customer') }}
)

select
    tenant_id,
    city,
    count(customer_id) as customer_count
from customers
group by tenant_id, city
order by customer_count desc