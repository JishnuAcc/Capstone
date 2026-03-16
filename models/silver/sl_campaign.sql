{{ config(materialized='incremental') }}

SELECT

    {{ standardize_date('record_date') }} AS record_date,

    campaign_id,

    {{ clean_string('campaign_name') }} AS campaign_name,
    {{ clean_string('campaign_type') }} AS campaign_type,
    {{ clean_string('channel') }} AS channel,
    {{ clean_string('description') }} AS description,

    {{ standardize_date('start_date') }} AS start_date,
    {{ standardize_date('end_date') }} AS end_date,
    {{ standardize_date('last_modified_date') }} AS last_modified_date,

    {{ clean_string('target_audience') }} AS target_audience,

    {{ extract_currency('budget') }} AS budget,
    {{ extract_currency('total_cost') }} AS total_cost,
    {{ extract_currency('total_revenue') }} AS total_revenue,

    {{ clean_string('roi_calculation') }} AS roi_calculation,

    /* Campaign duration */
    DATEDIFF(
        day,
        {{ standardize_date('start_date') }},
        {{ standardize_date('end_date') }}
    ) AS campaign_duration_days,

    /* Audience segment */
    CASE
        WHEN REGEXP_LIKE(target_audience, '.*60\\+.*') THEN 'Senior'
        WHEN REGEXP_LIKE(target_audience, '.*(18|18-25|18-35).*') THEN 'Young'
        WHEN REGEXP_LIKE(target_audience, '.*(25-50|30-50|36-55).*') THEN 'Middle-aged'
        ELSE 'Unknown'
    END AS audience_segment,

    /* Expected ROI calculation */
    (
        ROUND(1+({{ extract_currency('total_revenue') }} -     {{ extract_currency('total_cost') }})
        / NULLIF({{ extract_currency('total_cost') }},0),2)
    ) AS expected_roi,
    /* ROI validation */
    CASE
        WHEN TRY_TO_NUMBER(roi_calculation) IS NOT NULL
         AND 
                (roi_calculation) =
                
                (
                   ROUND(1+({{ extract_currency('total_revenue') }} -     {{ extract_currency('total_cost') }})
        / NULLIF({{ extract_currency('total_cost') }},0),2)
                )
             
        THEN TRUE
        ELSE FALSE
    END AS roi_validated

FROM {{ ref('br_campaign') }}

{% if is_incremental() %}
WHERE record_date > (SELECT MAX(record_date) FROM {{ this }})
{% endif %}