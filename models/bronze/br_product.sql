with source as (

    select *
    from {{ ref('product_rw') }} 

),

deduplicated as (

    select *,
        row_number() over (
            partition by product_id,record_date
            order by last_modified_date desc
        ) as rn

    from source

)

select *
from deduplicated
where rn = 1