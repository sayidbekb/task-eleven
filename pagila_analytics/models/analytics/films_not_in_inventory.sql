select
    f.tenant_id,
    f.film_id,
    f.title
from {{ ref('dim_film') }} f
left join {{ ref('stg_inventory') }} i
    on f.film_id = i.film_id
    and f.tenant_id = i.tenant_id
where i.inventory_id is null