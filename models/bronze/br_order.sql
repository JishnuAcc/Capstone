with source as (

    select *
    from {{ ref('order_rw') }} 

),

deduplicated as (

    select *,
        row_number() over (
            partition by order_id,record_date
            order by order_date desc
        ) as rn

    from source

)

select *
from deduplicated
where rn = 1