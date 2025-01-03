-- Purpose:
-- This model serves as the staging layer for profitability analysis.
-- It combines time-series price data and product attributes to create a unified dataset.
-- Key tasks include joining tables, selecting relevant columns, and preparing the data for further transformations.

-- Input Tables:
-- 1. bureau_of_labor_statistics_price_timeseries: Contains price data over time for various products and geographies.
-- 2. bureau_of_labor_statistics_price_attributes: Includes metadata about products, such as name, frequency, and units.

-- Key Features:
-- - Combines raw tables using a LEFT JOIN on the `variable` column.
-- - Adds product-level details to the price timeseries data.
-- - Prepares the data for downstream models in the pipeline.

-- Current Challenge: Duplicate Rows
-- - Duplicate rows exist in the output due to overlaps in the source data.
-- - This occurs when multiple entries for the same `geo_id`, `variable`, `date`, and `value` are present.
-- - I am actively working to remove these duplicates by implementing deduplication logic.
-- - Potential solutions include:
--     - Using DISTINCT to remove exact duplicates.
--     - Using ROW_NUMBER to rank and filter duplicate rows.
--     - Aggregating values (e.g., using AVG or MAX) to collapse duplicates.

-- Downstream Usage:
-- This model is the basis for further profitability calculations and analyses, ensuring
-- clean and consistent data before transformation.

-- Next Steps:
-- Deduplication logic will be applied here to ensure only unique rows are output.
-- This is a work in progress.


WITH price_timeseries AS (
    SELECT *
    FROM {{ source('pricing', 'bureau_of_labor_statistics_price_timeseries') }}
),

price_attributes AS (
    SELECT *
    FROM {{ source('pricing', 'bureau_of_labor_statistics_price_attributes') }}
),

deduplicated_timeseries AS (
    SELECT
        geo_id,
        variable,
        variable_name,
        value,
        date,
        ROW_NUMBER() OVER (PARTITION BY geo_id, variable, variable_name, date, value ORDER BY geo_id) AS row_num
    FROM price_timeseries
),

filtered_timeseries AS (
    SELECT *
    FROM deduplicated_timeseries
    WHERE row_num = 1
)

SELECT
    ft.geo_id,
    ft.variable,
    ft.variable_name,
    ft.value,
    ft.date,
    pa.product,
    pa.seasonally_adjusted,
    pa.frequency,
    pa.unit
FROM filtered_timeseries ft
LEFT JOIN price_attributes pa
ON ft.variable = pa.variable
