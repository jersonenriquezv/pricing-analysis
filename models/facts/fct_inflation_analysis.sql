-- This fact table stores inflation trends for each product over time.

-- Purpose:
-- - Calculate and store key inflation metrics, including:
--    - Average Consumer Price Index (`avg_cpi`).
--    - Previous Consumer Price Index (`prev_avg_cpi`).
--    - Inflation rate (`inflation_rate`).
-- - Enable efficient querying for inflation analysis and reporting.

-- Notes:
-- - Data is sourced from `stg_profitability_timeseries`.
-- - Uses `lag` to compute inflation rate as the percentage change in average CPI over time.

with inflation_trends as (
    select
        product,
        date,
        avg(value) as avg_cpi,
        lag(avg(value)) over (partition by product order by date) as prev_avg_cpi,
        case
            when lag(avg(value)) over (partition by product order by date) != 0 then
                ((avg(value) - lag(avg(value)) over (partition by product order by date)) 
                / lag(avg(value)) over (partition by product order by date)) * 100
            else null
        end as inflation_rate
    from {{ ref('stg_profitability_timeseries') }}
    group by product, date
)
select  
    *
from inflation_trends