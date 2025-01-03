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
