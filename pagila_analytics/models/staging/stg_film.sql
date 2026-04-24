SELECT
    {{ var("tenant_id") }} as tenant_id,
    film_id,
    title,
    length as film_length,
    rating,
    description,
    release_year,
    rental_rate,
    rental_duration,
    language_id,
    replacement_cost,
    last_update as last_updated
FROM {{ source('pagila', 'film') }}