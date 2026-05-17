{{ config(materialized='ephemeral') }}

WITH raw_data AS (
    SELECT 
        raw_json->'metadata'->>'tx_id' AS transaction_id,
        CAST(raw_json->'metadata'->>'timestamp' AS TIMESTAMP) AS created_at,
        raw_json->'metadata'->>'source_system' AS source_system,
        CAST(raw_json->'payload'->>'user_id' AS INTEGER) AS user_id,
        CAST(raw_json->'payload'->'financial'->>'amount' AS DECIMAL(15, 2)) AS amount,
        raw_json->'payload'->'financial'->>'currency' AS currency,
        raw_json->'payload'->'security'->>'ip_address' AS raw_ip_address,
        raw_json->'payload'->'security'->>'device_type' AS device_type,
        loaded_at
    FROM {{ source('public', 'landing_transactions') }}
),
deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY transaction_id ORDER BY loaded_at DESC) as rn
    FROM raw_data
)
SELECT * FROM deduplicated WHERE rn = 1