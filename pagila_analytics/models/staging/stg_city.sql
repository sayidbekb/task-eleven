SELECT
    {{ var("tenant_id") }} as tenant_id,
    city_id,
    city,
    country_id,
    last_update as last_updated
FROM {{ source('pagila', 'city') }}