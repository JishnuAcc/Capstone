with 

source as (

    select * from {{ source('raw_layer', 'campaign_rw') }}
    {{ incremental_filter('record_date') }}
),

renamed as (

    select
   TO_DATE(record_date) AS record_date,

    campaign_id::VARCHAR AS campaign_id,
    campaign_name::VARCHAR AS campaign_name,
    campaign_type::VARCHAR AS campaign_type,
    channel::VARCHAR AS channel,
    description::VARCHAR AS description,

    start_date::DATE AS start_date,
    end_date::DATE AS end_date,
    last_modified_date::DATE AS last_modified_date,

    target_audience::VARCHAR AS target_audience,

    budget::NUMBER AS budget,
    total_cost::NUMBER AS total_cost,
    total_revenue::NUMBER AS total_revenue,

    roi_calculation::VARCHAR AS roi_calculation
    from source

)

select * from renamed