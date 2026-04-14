with customers as (
    select *
    from {{ ref('dim_customer') }}
)

select
    city,
    count(customer_id) as customer_count
from customers
group by city
order by customer_count desc