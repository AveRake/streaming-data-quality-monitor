{{ config(materialized='table') }}

WITH bronze AS (
    SELECT * FROM {{ ref('bronze_transactions') }}
),
dq_rules AS (
    SELECT 
        transaction_id, created_at, source_system, user_id, amount, currency,
        {{ mask_ip('raw_ip_address') }} AS masked_ip,
        device_type,
        raw_ip_address,

        {{ apply_dq_rules('bronze_transactions') }} AS dq_status,

        CASE 
            WHEN amount > 50000 THEN 0.8
            WHEN device_type = 'Web' AND amount > 10000 THEN 0.5
            ELSE 0.1
        END as risk_score
    FROM bronze
)
SELECT * FROM dq_rules