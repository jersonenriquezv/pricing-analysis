-- This model calculates inflation trends for products over time.

-- Steps:
-- 1. Calculate the average Consumer Price Index (CPI) for each product and date.
-- 2. Use the `lag` function to get the previous average CPI (`prev_avg_cpi`) for each product.
-- 3. Compute the inflation rate as the percentage change between the current and previous average CPI.

-- Purpose:
-- - Analyze how prices change over time for each product.
-- - Provide insights into inflation trends.

-- Notes:
-- - The `where` clause ensures only rows with valid previous CPI values (`prev_avg_cpi`) are included.
-- - Input data comes from the `stg_profitability_timeseries` model.

with inflation_trends as (
    select
        product,
        date,
        avg(value) as avg_cpi,
        lag(avg(value)) over (partition by product order by date) as prev_avg_cpi,
        case
            when lag(avg(value)) over (partition by product order by date) is not null then
                ((avg(value) - lag(avg(value)) over (partition by product order by date)) / lag(avg(value)) over (partition by product order by date)) * 100
            else null
        end as inflation_rate
    from {{ ref('stg_profitability_timeseries') }}
    group by product, date
)
select *
from inflation_trends
where prev_avg_cpi is not null
