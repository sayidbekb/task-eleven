SELECT
    {{ var("tenant_id") }} as tenant_id,
    address_id,
    address,
    address2,
    district,
    city_id,
    postal_code,
    phone,
    last_update as last_updated
FROM {{ source('pagila', 'address') }}