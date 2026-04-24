-- models/marts/dim_film.sql

with film as (
    select *
    from {{ ref('stg_film') }}
),

film_category as (
    select *
    from {{ source('pagila', 'film_category') }}
),

category as (
    select *
    from {{ ref('stg_category') }}
)

select
    tenant_id,
    f.film_id,
    f.title,
    f.description,
    f.release_year,
    f.film_length,
    f.rating,
    f.rental_rate,
    c.category
from film f
left join film_category fc
    on f.film_id = fc.film_id
left join category c
    on fc.category_id = c.category_id
    and f.tenant_id = fc.tenant_id