select
    product,
    avg(avg_cost_change_percentage) as overall_avg_cost_change_percentage,
    max(max_cost_increase) as highest_cost_increase,
    min(min_cost_decrease) as lowest_cost_decrease
from {{ ref('int_cost_trends') }}
group by product
order by overall_avg_cost_change_percentage desc
