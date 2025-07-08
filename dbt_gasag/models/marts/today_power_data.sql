-- Combined table with actual and predicted data for today
WITH last_day_info AS (
    SELECT
        MAX(DATE(Timestamp)) as last_date,
        MAX(Timestamp) as last_timestamp
    FROM {{ ref('stg_kraftwerksdaten') }}
),

-- Get the last actual measurement time for each power plant
last_measurements AS (
    SELECT
        Kraftwerk,
        MAX(Timestamp) as last_actual_time
    FROM {{ ref('stg_kraftwerksdaten') }}
    WHERE leistung IS NOT NULL
    GROUP BY Kraftwerk
),

-- Calculate historical hourly averages for predictions
hourly_averages AS (
    SELECT
        Kraftwerk,
        hour_of_day,
        AVG(leistung) as avg_hourly_leistung
    FROM {{ ref('stg_kraftwerksdaten') }}
    WHERE leistung IS NOT NULL
    GROUP BY Kraftwerk, hour_of_day
),

-- Get actual data for the last day
actual_data AS (
    SELECT
        Kraftwerk,
        CAST(Timestamp AS TIMESTAMP) as Timestamp,
        hour_of_day,
        leistung as Leistung,
        NULL as Prognose
    FROM {{ ref('stg_kraftwerksdaten') }}
    WHERE DATE(Timestamp) = (SELECT last_date FROM last_day_info)
      AND leistung IS NOT NULL
),

-- Generate predictions only for hours AFTER the last actual measurement
predicted_data AS (
    SELECT
        ha.Kraftwerk,
        CAST(TIMESTAMP_ADD(
            TIMESTAMP_TRUNC((SELECT last_date FROM last_day_info), DAY),
            INTERVAL ha.hour_of_day HOUR
        ) AS TIMESTAMP) AS Timestamp,
        ha.hour_of_day,
        NULL as Leistung,
        ha.avg_hourly_leistung AS Prognose
    FROM hourly_averages ha
    JOIN last_measurements lm ON ha.Kraftwerk = lm.Kraftwerk
    WHERE ha.hour_of_day > EXTRACT(HOUR FROM lm.last_actual_time)
      AND NOT EXISTS (
        SELECT 1 FROM actual_data existing
        WHERE existing.Kraftwerk = ha.Kraftwerk
        AND existing.hour_of_day = ha.hour_of_day
    )
)

-- Combine actual and predicted data
SELECT
    Kraftwerk,
    Timestamp,
    hour_of_day,
    Leistung,
    Prognose,
    -- Add some useful metadata
    (SELECT last_date FROM last_day_info) as forecast_date
FROM (
    SELECT * FROM actual_data
    UNION ALL
    SELECT * FROM predicted_data
)
ORDER BY Kraftwerk, Timestamp
