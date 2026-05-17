{{ config(materialized='view') }}

WITH metrics AS (
    SELECT 
        -- 1. COMPLETENESS (Полнота)
        -- Считаем процент записей, где критичные поля (IP и User ID) заполнены
        SUM(CASE WHEN raw_ip_address IS NOT NULL AND user_id IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS completeness_score,

        -- 2. VALIDITY (Корректность)
        -- Процент записей, которые прошли наши проверки из справочника dq_rules и ML-модели
        SUM(CASE WHEN dq_status = 'Valid' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS validity_score,

        -- 3. TIMELINESS (Своевременность)
        -- Разница между временем транзакции (created_at) и временем загрузки в БД (loaded_at).
        -- Считаем "своевременными" те данные, которые дошли от Kafka до БД быстрее чем за 60 секунд.
        SUM(CASE WHEN EXTRACT(EPOCH FROM (loaded_at - created_at)) < 60 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS timeliness_score
    FROM {{ ref('silver_dq_transactions') }}
)
SELECT 
    ROUND(CAST(completeness_score AS NUMERIC), 2) AS completeness,
    ROUND(CAST(validity_score AS NUMERIC), 2) AS validity,
    ROUND(CAST(timeliness_score AS NUMERIC), 2) AS timeliness,
    -- ОБЩИЙ ИНДЕКС DQI (Среднее арифметическое трех метрик)
    ROUND(CAST((completeness_score + validity_score + timeliness_score) / 3 AS NUMERIC), 2) AS dqi_score
FROM metrics