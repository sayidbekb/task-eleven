with revenue as (
    select *
    from {{ ref('fact_revenue') }}
),

films as (
    select *
    from {{ ref('dim_film') }}
)

select
    r.tenant_id,
    f.category,
    round(sum(r.payment_amount), 2) as total_revenue
from revenue r
join films f on r.film_id = f.film_id
group by r.tenant_id, f.category
order by total_revenue desc