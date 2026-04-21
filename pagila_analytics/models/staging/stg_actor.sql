SELECT
    actor_id,
    first_name,
    last_name,
    last_update as last_updated
FROM {{ source('pagila', 'actor') }}