with all_dates as (
    -- grab all dates from rentals and payments
    select rental_date::date as the_date
    from {{ ref('stg_rental') }}
    union
    select payment_date::date as the_date
    from {{ ref('stg_payment') }}
)

select
    the_date as date_key,
    the_date as full_date,
    year(the_date) as year,
    month(the_date) as month,
    day(the_date) as day,
    dayofweek(the_date) as weekday,  -- 1=Sunday, 7=Saturday in Snowflake
    week(the_date) as week_of_year,
    case when dayofweek(the_date) in (1,7) then 'Weekend' else 'Weekday' end as day_type,
    to_char(the_date,'Mon') as month_name
from all_dates