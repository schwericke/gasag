-- Last day metrics using only real data (no forecasts)
WITH last_day_info AS (
    SELECT
        MAX(DATE(Timestamp)) as last_date
    FROM {{ ref('stg_kraftwerksdaten') }}
),

-- Get real data for the last day only
last_day_data AS (
    SELECT
        Kraftwerk,
        DATE(Timestamp) as date,
        -- Count total hours for the day (only real data)
        COUNT(*) AS total_hours,

        -- Calculate average grundlast (base load <= 50MWh) for this day
        ROUND(AVG(CASE WHEN NOT is_peak_load AND leistung IS NOT NULL THEN leistung END), 2) AS avg_grundlast,

        -- Calculate average spitzenlast (peak load > 50MWh) for this day
        ROUND(COALESCE(AVG(CASE WHEN is_peak_load THEN leistung END), 0), 2) AS avg_spitzenlast,

        -- Calculate percentages for this day
        ROUND(COUNT(CASE WHEN is_decreasing_zaehlerstand THEN 1 END) * 100.0 / COUNT(*), 2) AS pct_decreasing_readings,
        ROUND(COUNT(CASE WHEN is_peak_load THEN 1 END) * 100.0 / COUNT(*), 2) AS pct_peak_loads,

        -- Additional daily metrics
        ROUND(SUM(leistung), 2) AS daily_total_leistung

    FROM {{ ref('stg_kraftwerksdaten') }}
    WHERE DATE(Timestamp) = (SELECT last_date FROM last_day_info)
      AND leistung IS NOT NULL  -- Only real data, no forecasts
    GROUP BY Kraftwerk, DATE(Timestamp)
)

SELECT
    Kraftwerk,
    date,
    total_hours,
    avg_grundlast,
    avg_spitzenlast,
    pct_decreasing_readings,
    pct_peak_loads,
    daily_total_leistung
FROM last_day_data
ORDER BY Kraftwerk
