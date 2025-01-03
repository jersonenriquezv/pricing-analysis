-- This model summarizes inflation trends for each product.

-- Steps:
-- 1. Aggregate data from `int_inflation_analysis` to calculate:
--    - The average inflation rate (`avg_inflation_rate`) for each product.
--    - The maximum inflation spike (`max_inflation_spike`) for each product.
-- 2. Group the results by product to ensure clarity and consistency.

-- Purpose:
-- - Provide a high-level summary of inflation trends for products.
-- - Identify products with the highest average inflation rates and significant spikes.

-- Notes:
-- - The output is ordered by `avg_inflation_rate` in descending order to highlight products with the highest inflation rates.
-- - Input data comes from the intermediate model `int_inflation_analysis`, which calculates inflation rates at a detailed level.
-- - Ensure `int_inflation_analysis` is accurate and free of duplicates to maintain reliable results.

select
    product,
    avg(inflation_rate) as avg_inflation_rate,
    max(inflation_rate) as max_inflation_spike
from {{ ref('int_inflation_analysis') }}
group by product
order by avg_inflation_rate desc
