{{ config(materialized='table') }}

WITH bronze AS (
    SELECT * FROM {{ ref('bronze_transactions') }}
),
dq_rules AS (
    SELECT 
        transaction_id, created_at, source_system, user_id, amount, currency,
        {{ mask_ip('raw_ip_address') }} AS masked_ip,
        device_type,
        CASE 
            WHEN amount <= 0 THEN 'CRITICAL: Negative or Zero Amount'
            WHEN amount > 1000000 THEN 'WARNING: Suspiciously Huge Amount (Fraud)'
            WHEN currency NOT IN ('RUB', 'USD', 'EUR', 'KZT') THEN 'ERROR: Invalid Currency Code'
            WHEN raw_ip_address IS NULL THEN 'ERROR: Missing IP Address'
            WHEN user_id < 1000 OR user_id > 99999 THEN 'ERROR: Invalid User ID Format'
            ELSE 'Valid'
        END as dq_status,
        CASE 
            WHEN amount > 50000 THEN 0.8
            WHEN device_type = 'Web' AND amount > 10000 THEN 0.5
            ELSE 0.1
        END as risk_score
    FROM bronze
)
SELECT * FROM dq_rules