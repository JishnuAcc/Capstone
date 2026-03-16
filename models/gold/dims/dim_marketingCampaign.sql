{{ config(materialized='table') }}

SELECT

    MD5(CONCAT(campaign_id)) AS campaign_key,

    campaign_id,
    audience_segment AS target_audience_segment,
    budget,
    campaign_duration_days AS duration,
    expected_roi AS roi,
    start_date,
    end_date

FROM {{ ref('sl_campaign') }}