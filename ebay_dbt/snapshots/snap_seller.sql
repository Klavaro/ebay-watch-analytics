{% snapshot snap_seller %}
{{
    config(
        target_schema='snapshots',
        unique_key='seller_username_cleaned',
        strategy='timestamp',
        updated_at='load_timestamp'
    )
}}

select
    seller_username_cleaned,
    seller_feedback_score,
    seller_feedback_percentage,
    load_timestamp
from {{ ref('stg_ebay_items') }}
where seller_username_cleaned != 'ANONYMOUS'
{% endsnapshot %}
