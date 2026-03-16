{% snapshot customer_snapshot %}

{{
config(
    target_schema='CP_GOLD',
    unique_key='customer_id',

    strategy='check',
    check_cols=[
        'full_name',
        'email',
        'phone',
        'address',
        'customer_segment',
        'income_bracket',
        'loyalty_tier'
    ]
)
}}

SELECT
    customer_id,
    full_name,
    email,
    phone,
    address,
    customer_segment,
    income_bracket,
    loyalty_tier,
    registration_date
FROM {{ ref('sl_customer') }}

{% endsnapshot %}