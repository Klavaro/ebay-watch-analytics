-- Purpose: Quantify seller reputation with a weighted score to inform sourcing decisions or partnership evaluations.
-- Business Questions:
--  Which sellers are most reliable?
--  How do reputation metrics influence pricing?
with base as (
    select distinct
        seller_sk,
        seller_feedback_score,
        seller_feedback_percentage
    from {{ ref('dim_seller') }}
),

scored as (
    select
        seller_sk,
        {{ reputation_score('seller_feedback_score', 'seller_feedback_percentage') }} as seller_reputation_score
    from base
)

select * from scored
