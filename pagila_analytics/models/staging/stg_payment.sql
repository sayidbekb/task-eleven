SELECT
    {{ var("tenant_id") }} as tenant_id,
    payment_id,
    customer_id,
    staff_id,
    rental_id,
    amount,
    payment_date
FROM {{ source('pagila', 'payment') }}