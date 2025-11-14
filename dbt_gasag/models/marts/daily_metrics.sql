WITH staging_data AS (
    SELECT * FROM {{ ref('stg_kraftwerksdaten') }}
),

-- Daily metrics for each kraftwerk and each day
daily_metrics AS (
    SELECT
        Kraftwerk,
        DATE(Timestamp) as date,
        -- Count total hours for the day
        COUNT(*) AS total_hours,

                -- Calculate average grundlast (base load <= 50MWh) for this day
        ROUND(AVG(CASE WHEN NOT is_peak_load AND leistung IS NOT NULL THEN leistung END), 2) AS avg_grundlast,

        -- Calculate average spitzenlast (peak load > 50MWh) for this day
        ROUND(AVG(CASE WHEN is_peak_load THEN leistung END), 2) AS avg_spitzenlast,

        -- Calculate percentages for this day
        ROUND(COUNT(CASE WHEN is_decreasing_zaehlerstand THEN 1 END) * 100.0 / COUNT(*), 2) AS pct_decreasing_readings,
        ROUND(COUNT(CASE WHEN is_peak_load THEN 1 END) * 100.0 / COUNT(*), 2) AS pct_peak_loads,

        -- Additional daily metrics
        ROUND(SUM(leistung), 2) AS daily_total_leistung

    FROM staging_data
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

FROM daily_metrics
ORDER BY Kraftwerk, date
