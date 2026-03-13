with 

source as (

    select * from {{ source('raw_layer', 'campaign_rw') }}
    {{ incremental_filter('record_date') }}
),

renamed as (

    select
        record_date,
        campaign_id,
        campaign_name,
        campaign_type,
        channel,
        description,
        start_date,
        end_date,
        last_modified_date,
        target_audience,
        budget,
        total_cost,
        total_revenue,
        roi_calculation

    from source

)

select * from renamed