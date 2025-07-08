
  
    

    create or replace table `gasag-465208`.`analysis`.`kraftwerke_metrics`
      
    
    

    OPTIONS()
    as (
      WITH staging_data AS (
    SELECT * FROM `gasag-465208`.`analysis`.`stg_kraftwerksdaten`
),

-- Daily aggregations first
daily_metrics AS (
    SELECT
        Kraftwerk,
        DATE(Timestamp) as date,
        -- Daily total load (sum of all Leistung for the day)
        SUM(leistung) AS daily_total_load,
        -- Daily average load (average of all Leistung for the day)
        AVG(leistung) AS daily_avg_load,
        -- Count of peak loads for the day
        COUNT(CASE WHEN is_peak_load THEN 1 END) AS daily_peak_loads,
        -- Count of decreasing meter readings for the day
        COUNT(CASE WHEN is_decreasing_zaehlerstand THEN 1 END) AS daily_decreasing_readings,
        -- Total hours with data for the day
        COUNT(*) AS daily_total_hours
    FROM staging_data
    WHERE NOT is_last_day
    GROUP BY Kraftwerk, DATE(Timestamp)
),

-- Historical metrics (averages across all days)
historical_metrics AS (
    SELECT
        s.Kraftwerk,
        -- Durchschnittliche Grundlast (average base load per hour, excluding NULLs)
        AVG(CASE WHEN NOT s.is_peak_load THEN s.leistung END) AS durchschnittliche_grundlast,

        -- Durchschnittliche Spitzenlast (average peak load per hour, excluding NULLs)
        AVG(CASE WHEN s.is_peak_load THEN s.leistung END) AS durchschnittliche_spitzenlast,

        -- Durchschnittliche Gesamtlast pro Tag (average daily total load)
        AVG(d.daily_total_load) AS durchschnittliche_gesamtlast_pro_tag,

        -- Quote an Spitzenlasten (percentage of peak loads across all days)
        SUM(d.daily_peak_loads) * 100.0 / SUM(d.daily_total_hours) AS quote_spitzenlasten_prozent,

        -- Quote an Rückläufigen Zählerständen (percentage of decreasing meter readings across all days)
        SUM(d.daily_decreasing_readings) * 100.0 / SUM(d.daily_total_hours) AS quote_ruecklaeufige_zaehlerstaende_prozent

    FROM staging_data s
    LEFT JOIN daily_metrics d ON s.Kraftwerk = d.Kraftwerk AND DATE(s.Timestamp) = d.date
    WHERE NOT s.is_last_day
    GROUP BY s.Kraftwerk
),

-- Expected performance from today_power_data (sum of actual + predicted)
today_expected AS (
    SELECT
        Kraftwerk,
        -- Sum all values (Leistung + Prognose) for the day
        SUM(COALESCE(Leistung, 0) + COALESCE(Prognose, 0)) AS erwartete_leistung
    FROM `gasag-465208`.`analysis`.`today_power_data`
    GROUP BY Kraftwerk
)

SELECT
    h.Kraftwerk,
    h.durchschnittliche_grundlast,
    h.durchschnittliche_spitzenlast,
    h.durchschnittliche_gesamtlast_pro_tag,
    h.quote_spitzenlasten_prozent,
    h.quote_ruecklaeufige_zaehlerstaende_prozent,
    t.erwartete_leistung
FROM historical_metrics h
LEFT JOIN today_expected t ON h.Kraftwerk = t.Kraftwerk
    );
  