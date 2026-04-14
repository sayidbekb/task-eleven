with rentals as (
    select *
    from {{ ref('fact_rental') }}
),

films as (
    select *
    from {{ ref('dim_film') }}
)

select
    f.category,
    sum(r.rental_duration_days * 24) as total_hours,
    avg(r.rental_duration_days * 24) as avg_hours
from rentals r
join films f on r.film_id = f.film_id
group by f.category
order by total_hours desc