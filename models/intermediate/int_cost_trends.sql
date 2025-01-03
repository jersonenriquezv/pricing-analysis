-- Purpose:
-- This intermediate model calculates cost trends over time for each product.
-- It analyzes average costs, total costs, and percentage changes in costs over time.

-- Input:
-- Data is sourced from the `stg_profitability_timeseries` model, which provides cleaned and joined price data.

-- Key Steps:
-- 1. cost_trends:
--    - Groups data by product and date.
--    - Calculates average cost (`avg_cost`) and total cost (`total_cost`) for each product on a given date.
-- 2. trend_analysis:
--    - Calculates trends by:
--      - Comparing the current average cost to the previous average cost.
--      - Calculating absolute cost changes (`cost_change`) and percentage changes (`cost_change_percentage`).
-- 3. Final Output:
--    - Aggregates the percentage changes across products to provide:
--      - Average percentage change (`avg_cost_change_percentage`).
--      - Maximum increase (`max_cost_increase`).
--      - Maximum decrease (`min_cost_decrease`).

-- Downstream Usage:
-- - This model provides input for summary tables and visualizations.
-- - It helps identify products with significant cost changes over time.

-- Note:
-- - Ensure that the upstream staging model (`stg_profitability_timeseries`) is free of duplicates to maintain accuracy.
-- - Results are grouped and ordered by `avg_cost_change_percentage` to highlight significant trends.

with cost_trends as (
    select
        product,
        date,
        avg(value) as avg_cost,
        sum(value) as total_cost
    from {{ ref('stg_profitability_timeseries') }}
    group by product, date
),
trend_analysis as (
    select
        product,
        date,
        avg_cost,
        total_cost,
        lag(avg_cost) over (partition by product order by date) as prev_avg_cost,
        avg_cost - lag(avg_cost) over (partition by product order by date) as cost_change,
        case
            when lag(avg_cost) over (partition by product order by date) != 0 then
                (avg_cost - lag(avg_cost) over (partition by product order by date)) / lag(avg_cost) over (partition by product order by date) * 100
            else null
        end as cost_change_percentage
    from cost_trends
)
select
    product,
    avg(cost_change_percentage) as avg_cost_change_percentage,
    max(cost_change_percentage) as max_cost_increase,
    min(cost_change_percentage) as min_cost_decrease
from trend_analysis
group by product
order by avg_cost_change_percentage desc

