{{ config(materialized='table') }}

WITH campaign_base AS (

    SELECT
        campaign_key,
        campaign_id,
        start_date,
        end_date,
        budget
    FROM {{ ref('dim_marketingCampaign') }}

),

sales_base AS (

    SELECT
        fs.campaign_key,
        fs.customer_key,
        dc.customer_id,
        fs.date_key,
        fs.total_sales_amount
    FROM {{ ref('fact_sales') }} fs
    LEFT JOIN {{ ref('dim_customer') }} dc
        ON fs.customer_key = dc.customer_key

),

campaign_sales AS (

    SELECT
        sb.campaign_key,
        sb.customer_key,
        sb.customer_id,
        sb.date_key,
        sb.total_sales_amount,
        cb.start_date,
        cb.end_date,
        cb.budget
    FROM sales_base sb
    JOIN campaign_base cb
        ON sb.campaign_key = cb.campaign_key

),

sales_in_window AS (

    SELECT
        sb.campaign_key,
        sb.customer_key,
        sb.customer_id,
        sb.date_key,
        sb.total_sales_amount,
        cb.start_date,
        cb.end_date,
        cb.budget
    FROM sales_base sb
    JOIN campaign_base cb
        ON sb.campaign_key = cb.campaign_key
    JOIN {{ ref('dim_date') }} dd
        ON sb.date_key = dd.date_key
    WHERE dd.full_date BETWEEN cb.start_date AND cb.end_date

),

total_sales_influenced AS (

    SELECT
        campaign_key,
        SUM(total_sales_amount) AS total_sales_influenced
    FROM sales_in_window
    GROUP BY campaign_key

),

new_customers AS (

    SELECT
        siw.campaign_key,
        COUNT(DISTINCT siw.customer_key) AS new_customers_acquired
    FROM sales_in_window siw
    WHERE siw.customer_id NOT IN (
        SELECT DISTINCT customer_id
        FROM {{ ref('fact_sales') }} fs
        JOIN {{ ref('dim_customer') }} dc
            ON fs.customer_key = dc.customer_key
        JOIN {{ ref('dim_date') }} dd
            ON fs.date_key = dd.date_key
        WHERE dd.full_date < siw.start_date
    )
    GROUP BY siw.campaign_key

),

customer_purchase_flags AS (

    SELECT
        customer_key,
        campaign_key,
        COUNT(*) AS purchase_count
    FROM sales_in_window
    GROUP BY customer_key, campaign_key

),

repeat_purchase_metrics AS (

    SELECT
        campaign_key,
        COUNT(DISTINCT CASE WHEN purchase_count > 1 THEN customer_key END) * 100
        / NULLIF(COUNT(DISTINCT customer_key),0) AS repeat_purchase_rate
    FROM customer_purchase_flags
    GROUP BY campaign_key

),

roi_metrics AS (

    SELECT
        tsi.campaign_key,
        tsi.total_sales_influenced,
        cb.budget AS total_campaign_cost,
        (tsi.total_sales_influenced - cb.budget) / NULLIF(cb.budget,0) * 100 AS roi_percentage
    FROM total_sales_influenced tsi
    JOIN campaign_base cb
        ON tsi.campaign_key = cb.campaign_key

),

final AS (

    SELECT
        cb.campaign_key,
        dd.date_key,
        tsi.total_sales_influenced,
        nc.new_customers_acquired,
        rpm.repeat_purchase_rate,
        rm.roi_percentage

    FROM campaign_base cb

    LEFT JOIN total_sales_influenced tsi
        ON cb.campaign_key = tsi.campaign_key

    LEFT JOIN new_customers nc
        ON cb.campaign_key = nc.campaign_key

    LEFT JOIN repeat_purchase_metrics rpm
        ON cb.campaign_key = rpm.campaign_key

    LEFT JOIN roi_metrics rm
        ON cb.campaign_key = rm.campaign_key

    LEFT JOIN {{ ref('dim_date') }} dd
        ON dd.full_date BETWEEN cb.start_date AND cb.end_date

)

SELECT *
FROM final