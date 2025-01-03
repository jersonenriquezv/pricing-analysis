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