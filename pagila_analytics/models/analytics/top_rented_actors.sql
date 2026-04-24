with rentals as (
    select *
    from {{ ref('fact_rental') }}
),

films as (
    select *
    from {{ ref('dim_film') }}
),

actors as (
    select *
    from {{ ref('dim_actor') }}
),

film_actor as (
    select *
    from {{ ref('int_film_actor_bridge') }}
)

select
     r.tenant_id,
    a.first_name,
    a.last_name,
    count(r.rental_id) as total_rentals
from rentals r
join films f on r.film_id = f.film_id
join film_actor fa on f.film_id = fa.film_id
join actors a on fa.actor_id = a.actor_id
group by r.tenant_id, a.first_name, a.last_name
order by total_rentals desc
limit 10