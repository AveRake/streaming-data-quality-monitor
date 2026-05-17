{{ config(materialized='view') }}

WITH silver AS (
    SELECT * FROM {{ ref('silver_dq_transactions') }}
)
SELECT 
    user_id,
    COUNT(transaction_id) as total_transactions,
    SUM(CASE WHEN dq_status = 'Valid' THEN amount ELSE 0 END) as total_valid_spent,
    MAX(created_at) as last_activity_date,
    ROUND(SUM(CASE WHEN dq_status != 'Valid' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as anomaly_percentage,
    AVG(risk_score) as avg_risk_score
FROM silver
WHERE user_id IS NOT NULL
GROUP BY user_id