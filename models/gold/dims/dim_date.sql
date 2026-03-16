{{ config(materialized='table') }}

WITH date_spine AS (

SELECT
    DATEADD(day, SEQ4(), '2015-01-01') AS full_date
FROM TABLE(GENERATOR(ROWCOUNT => 5000))

)

SELECT

    TO_NUMBER(TO_CHAR(full_date,'YYYYMMDD')) AS date_key,

    full_date,

    YEAR(full_date) AS year,

    QUARTER(full_date) AS quarter,

    MONTH(full_date) AS month,

    WEEK(full_date) AS week,

    DAYOFWEEK(full_date) AS day_of_week,

    CASE
        WHEN DAYOFWEEK(full_date) IN (1,7) THEN TRUE
        ELSE FALSE
    END AS holiday_flag,

    CASE
        WHEN MONTH(full_date) IN (12,1,2) THEN 'Winter'
        WHEN MONTH(full_date) IN (3,4,5) THEN 'Spring'
        WHEN MONTH(full_date) IN (6,7,8) THEN 'Summer'
        ELSE 'Autumn'
    END AS season

FROM date_spine