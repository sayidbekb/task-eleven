with rental as (
    select *
    from {{ ref('int_rental_facts') }}
)

select
    tenant_id,
    rental_id,
    customer_id,
    film_id,
    rental_date,
    return_date,
    rental_duration_days,
    is_returned
from rental