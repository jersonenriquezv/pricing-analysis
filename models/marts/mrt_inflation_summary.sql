select
    product,
    avg(inflation_rate) as avg_inflation_rate,
    max(inflation_rate) as max_inflation_spike
from {{ ref('int_inflation_analysis') }}
group by product
order by avg_inflation_rate desc
