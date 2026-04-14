SELECT
    inventory_id,
    film_id,
    store_id,
    last_update as last_updated
FROM {{ source('pagila', 'inventory') }}