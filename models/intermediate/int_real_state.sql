with residential_prices as (
    select
        product,
        date,
        avg(value) as avg_price
    from {{ ref('stg_profitability_timeseries') }}
    where lower(product) like '%real estate%' -- Filter for real estate products
    group by product, date
),
price_analysis as (
    select
        product,
        date,
        avg_price,
        lag(avg_price) over (partition by product order by date) as prev_avg_price,
        avg_price - lag(avg_price) over (partition by product order by date) as price_change,
        case
            when lag(avg_price) over (partition by product order by date) != 0 then
                ((avg_price - lag(avg_price) over (partition by product order by date)) 
                / lag(avg_price) over (partition by product order by date)) * 100
            else null
        end as price_change_percentage
    from residential_prices
)
select
    product,
    date,
    avg_price,
    price_change,
    price_change_percentage
from price_analysis
