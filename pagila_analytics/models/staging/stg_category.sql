SELECT
   category_id,
   name as category,
   last_update as last_updated
FROM {{ source('pagila', 'category') }}