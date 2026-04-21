with customer as (
    select *
    from {{ ref('stg_customer') }}
),

address as (
    select *
    from {{ ref('stg_address') }}
),

city as (
    select *
    from {{ ref('stg_city') }}
)

select
    c.customer_id,
    concat(c.first_name, ' ', c.last_name) as customer_full_name,
    c.email,
    c.is_active as active_status,
    c.create_date as customer_since,
    a.address,
    a.district,
    a.postal_code,
    ci.city

from customer c
left join address a
    on c.address_id = a.address_id
left join city ci
    on a.city_id = ci.city_id