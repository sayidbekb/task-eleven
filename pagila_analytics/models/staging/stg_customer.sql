SELECT
    {{ var("tenant_id") }} as tenant_id,
    customer_id,
    store_id,
    first_name,
    last_name,
    email,
    address_id,
    activebool as is_active,
    active as active_status,
    create_date,
    last_update as last_updated
FROM {{ source('pagila', 'customer') }}