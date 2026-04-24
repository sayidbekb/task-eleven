with films as (
    select *
    from {{ ref('dim_film') }}
)

select
    category,
    count(film_id) as film_count
from films
group by category
order by film_count desc