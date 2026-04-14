with film_actor as (
    select *
    from {{ source('pagila', 'film_actor') }}
),

film as (
    select *
    from {{ ref('stg_film') }}
),

actor as (
    select *
    from {{ ref('stg_actor') }}
)

select
    fa.actor_id,
    fa.film_id,
    f.title as film_title,
    concat(a.first_name, ' ', a.last_name) as actor_full_name,
    f.release_year,
    f.rating,
    f.rental_rate
from film_actor fa
join film f
    on fa.film_id = f.film_id
join actor a
    on fa.actor_id = a.actor_id