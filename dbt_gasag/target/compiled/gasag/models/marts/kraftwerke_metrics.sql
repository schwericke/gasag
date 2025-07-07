WITH staging_data AS (
    SELECT * FROM `gasag-465208`.`raw`.`stg_kraftwerksdaten`
),

-- Historical metrics (excluding last day)
historical_metrics AS (
    SELECT
        Kraftwerk,
        -- Durchschnittliche Grundlast (average base load, excluding NULLs)
        AVG(CASE WHEN NOT is_peak_load THEN leistung END) AS durchschnittliche_grundlast,

        -- Durchschnittliche Spitzenlast (average peak load, excluding NULLs)
        AVG(CASE WHEN is_peak_load THEN leistung END) AS durchschnittliche_spitzenlast,

        -- Durchschnittliche Gesamtlast pro Tag (average total load, excluding NULLs)
        AVG(leistung) AS durchschnittliche_gesamtlast_pro_tag,

        -- Quote an Spitzenlasten (percentage of peak loads)
        COUNT(CASE WHEN is_peak_load THEN 1 END) * 100.0 / COUNT(CASE WHEN leistung IS NOT NULL THEN 1 END) AS quote_spitzenlasten_prozent,

        -- Quote an Rückläufigen Zählerständen (percentage of decreasing meter readings)
        COUNT(CASE WHEN is_decreasing_zaehlerstand THEN 1 END) * 100.0 / COUNT(*) AS quote_ruecklaeufige_zaehlerstaende_prozent

    FROM staging_data
    WHERE NOT is_last_day
    GROUP BY Kraftwerk
),

-- Last day metrics
last_day_metrics AS (
    SELECT
        Kraftwerk,
        -- Sum of Leistung for the last day (excluding NULLs)
        SUM(leistung) AS last_day_leistung_sum,
        -- Count of hours with valid data for the last day
        COUNT(CASE WHEN leistung IS NOT NULL THEN 1 END) AS last_day_hours_with_data,
        -- Average Leistung for the last day (excluding NULLs)
        AVG(leistung) AS last_day_durchschnitt_leistung
    FROM staging_data
    WHERE is_last_day
    GROUP BY Kraftwerk
),

-- Expected vs actual comparison for last day
last_day_deviation AS (
    SELECT
        h.Kraftwerk,
        h.durchschnittliche_gesamtlast_pro_tag,
        l.last_day_leistung_sum,
        l.last_day_hours_with_data,
        l.last_day_durchschnitt_leistung,
        -- Expected Leistung (summierte Leistung * 24 / überlieferte Stunden)
        CASE
            WHEN l.last_day_hours_with_data > 0
            THEN (l.last_day_leistung_sum * 24.0) / l.last_day_hours_with_data
            ELSE NULL
        END AS erwartete_leistung,
        -- Tagesabweichung (Expected - Actual average)
        CASE
            WHEN l.last_day_hours_with_data > 0
            THEN ((l.last_day_leistung_sum * 24.0) / l.last_day_hours_with_data) - h.durchschnittliche_gesamtlast_pro_tag
            ELSE NULL
        END AS tagesabweichung
    FROM historical_metrics h
    LEFT JOIN last_day_metrics l ON h.Kraftwerk = l.Kraftwerk
)

SELECT
    h.Kraftwerk,
    h.durchschnittliche_grundlast,
    h.durchschnittliche_spitzenlast,
    h.durchschnittliche_gesamtlast_pro_tag,
    h.quote_spitzenlasten_prozent,
    h.quote_ruecklaeufige_zaehlerstaende_prozent,
    l.erwartete_leistung,
    l.tagesabweichung
FROM historical_metrics h
LEFT JOIN last_day_deviation l ON h.Kraftwerk = l.Kraftwerk