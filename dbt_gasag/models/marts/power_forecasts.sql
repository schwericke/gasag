-- Generate simple predictions using historical hourly averages
WITH last_day_info AS (
    SELECT
        MAX(DATE(Timestamp)) as last_date,
        MAX(Timestamp) as last_timestamp
    FROM {{ ref('stg_kraftwerksdaten') }}
),

-- Calculate historical hourly averages for each power plant
hourly_averages AS (
    SELECT
        Kraftwerk,
        hour_of_day,
        AVG(leistung) as avg_hourly_leistung,
        COUNT(*) as sample_count
    FROM {{ ref('stg_kraftwerksdaten') }}
    WHERE leistung IS NOT NULL
    GROUP BY Kraftwerk, hour_of_day
),

-- Generate missing hours for the last day
missing_hours AS (
    SELECT
        ha.Kraftwerk,
        TIMESTAMP_ADD(
            TIMESTAMP_TRUNC((SELECT last_date FROM last_day_info), DAY),
            INTERVAL ha.hour_of_day HOUR
        ) AS forecast_timestamp,
        ha.hour_of_day,
        ha.avg_hourly_leistung AS prognose
    FROM hourly_averages ha
    WHERE NOT EXISTS (
        SELECT 1 FROM {{ ref('stg_kraftwerksdaten') }} existing
        WHERE existing.Kraftwerk = ha.Kraftwerk
        AND CAST(existing.Timestamp AS TIMESTAMP) = CAST(TIMESTAMP_ADD(
            TIMESTAMP_TRUNC((SELECT last_date FROM last_day_info), DAY),
            INTERVAL ha.hour_of_day HOUR
        ) AS TIMESTAMP)
    )
)

SELECT
    Kraftwerk,
    forecast_timestamp AS Timestamp,
    hour_of_day,
    prognose,
    TRUE AS is_forecast
FROM missing_hours
ORDER BY Kraftwerk, forecast_timestamp
