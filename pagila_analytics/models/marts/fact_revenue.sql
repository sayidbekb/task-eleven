with rental as (
    select *
    from {{ ref('int_rental_facts') }}
)

select
    rental_id,
    customer_id,
    film_id,
    payment_amount,
    payment_date,
    revenue
from rental