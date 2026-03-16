with source as (

    select *
    from {{ ref('campaign_rw') }} 

),

deduplicated as (

    select     
        record_date,
        campaign_id::VARCHAR AS campaign_id,
        campaign_name::VARCHAR AS campaign_name,
        campaign_type::VARCHAR AS campaign_type,
        channel::VARCHAR AS channel,
        description::VARCHAR AS description,

        start_date::DATE AS start_date,
        end_date::DATE AS end_date,
        last_modified_date::DATE AS last_modified_date,

        target_audience::VARCHAR AS target_audience,

        budget::VARCHAR AS budget,
        total_cost::VARCHAR AS total_cost,
        total_revenue::VARCHAR AS total_revenue,

        roi_calculation::VARCHAR AS roi_calculation,
            row_number() over (
            partition by campaign_id,record_date
            order by last_modified_date desc
        ) as rn

    from source

)

select *
from deduplicated
where rn = 1