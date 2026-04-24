with actor as (
    select *
    from {{ ref('stg_actor') }}
)

select
    tenant_id,
    actor_id,
    first_name,
    last_name,
    last_updated
from actor