with actor as (
    select *
    from {{ ref('stg_actor') }}
)

select
    actor_id,
    first_name,
    last_name,
    last_updated
from actor