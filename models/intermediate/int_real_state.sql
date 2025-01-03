-- This model analyzes residential real estate prices over time.

-- Steps:
-- 1. Filter for products related to real estate from `stg_profitability_timeseries`.
-- 2. Calculate the average price (`avg_price`) for each product and date.
-- 3. Use the `lag` function to get the previous average price (`prev_avg_price`) for each product.
-- 4. Compute the absolute price change (`price_change`) and percentage change (`price_change_percentage`).

-- Purpose:
-- - Track price trends for real estate-related products over time.
-- - Provide insights into price fluctuations and changes.

-- Current Status:
-- - This model is still a work in progress and not fully functional yet.
-- - Challenges include ensuring the `where` clause filters real estate products correctly and validating the results.
-- - Adjustments are being made to refine the filtering logic and confirm the accuracy of the output.

-- Notes:
-- - The `where` clause filters for products containing "real estate".
-- - Input data from `stg_profitability_timeseries` must be clean and consistent.
-- - Additional testing and refinement are needed before this model is finalized.

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
