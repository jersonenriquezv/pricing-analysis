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
    *
from trend_analysis