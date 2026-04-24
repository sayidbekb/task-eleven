-- models/intermediate/int_rental_facts.sql

with rental as (
    select *
    from {{ ref('stg_rental') }}
),
inventory as (
    select *
    from {{ ref('stg_inventory') }}
),
film as (
    select *
    from {{ ref('stg_film') }}
),
payment as (
    select *
    from {{ ref('stg_payment') }}
)

select
    r.tenant_id,
    r.rental_id,
    r.customer_id,
    r.staff_id,
    i.film_id,
    f.title as film_title,
    r.rental_date,
    r.return_date,
    p.amount as payment_amount,
    p.payment_date,
    datediff('day', r.rental_date, r.return_date) as rental_duration_days,
    case
        when r.return_date is null then false
        else true
    end as is_returned,
    case
        when p.amount is null then 0
        else p.amount
    end as revenue
from rental r
join inventory i 
    on r.inventory_id = i.inventory_id
    and r.tenant_id = i.tenant_id
left join film f
    on i.film_id = f.film_id
    and i.tenant_id = f.tenant_id
left join payment p 
    on r.rental_id = p.rental_id
    and r.tenant_id = p.tenant_id