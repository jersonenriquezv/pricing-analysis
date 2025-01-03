-- This model summarizes profitability trends for each product.

-- Steps:
-- 1. Aggregate data from `int_cost_trends` to calculate:
--    - The overall average cost change percentage (`overall_avg_cost_change_percentage`).
--    - The highest recorded cost increase (`highest_cost_increase`).
--    - The lowest recorded cost decrease (`lowest_cost_decrease`).
-- 2. Group the results by product to ensure aggregated values are calculated correctly.

-- Purpose:
-- - Provide a comprehensive summary of profitability trends for each product.
-- - Highlight products with the most significant cost increases and decreases.

-- Notes:
-- - The output is ordered by `overall_avg_cost_change_percentage` in descending order to emphasize products with the largest average cost changes.
-- - Input data comes from the intermediate model `int_cost_trends`, which processes detailed cost trends for each product.
-- - Ensure `int_cost_trends` is accurate and consistent to maintain reliability of the summary results.


select
    product,
    avg(avg_cost_change_percentage) as overall_avg_cost_change_percentage,
    max(max_cost_increase) as highest_cost_increase,
    min(min_cost_decrease) as lowest_cost_decrease
from {{ ref('int_cost_trends') }}
group by product
order by overall_avg_cost_change_percentage desc
