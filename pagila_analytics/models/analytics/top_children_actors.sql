with films as (
    select *
    from {{ ref('dim_film') }}
    where lower(category) in ('children')
),

film_actor as (
    select *
    from {{ ref('int_film_actor_bridge') }}
),

actors as (
    select *
    from {{ ref('dim_actor') }}
)

select
    f.tenant_id,
    r.tenant_id,
    a.first_name,
    a.last_name,
    count(*) as film_count
from films f
join film_actor fa 
on f.film_id = fa.film_id
and f.tenant_id = fa.tenant_id
join actors a on fa.actor_id = a.actor_id
group by f.tenant_id, a.first_name, a.last_name
order by film_count desc
limit 10