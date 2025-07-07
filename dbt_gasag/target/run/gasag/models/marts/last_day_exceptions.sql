
  
    

    create or replace table `gasag-465208`.`raw`.`last_day_exceptions`
      
    
    

    OPTIONS()
    as (
      WITH raw_data AS (
    SELECT
        Kraftwerk,
        -- Parse German date format and convert to TIMESTAMP
        PARSE_TIMESTAMP('%d.%m.%Y %H:%M', Timestamp) as Timestamp,
        Zaehlerstand,
        -- Calculate raw Leistung (including negative values)
        Zaehlerstand - LAG(Zaehlerstand) OVER (PARTITION BY Kraftwerk ORDER BY PARSE_TIMESTAMP('%d.%m.%Y %H:%M', Timestamp)) AS raw_leistung
    FROM `gasag-465208`.`raw`.`kraftwerksdaten_raw`
),

max_date AS (
    SELECT MAX(DATE(PARSE_TIMESTAMP('%d.%m.%Y %H:%M', Timestamp))) as last_date
    FROM `gasag-465208`.`raw`.`kraftwerksdaten_raw`
),

last_day_data AS (
    SELECT
        Kraftwerk,
        Timestamp,
        EXTRACT(HOUR FROM Timestamp) AS hour_of_day,
        raw_leistung,
        -- Flag for peak load (> 50MW)
        CASE
            WHEN raw_leistung > 50 THEN TRUE
            ELSE FALSE
        END AS is_peak_load,
        -- Flag for negative Leistung
        CASE
            WHEN raw_leistung < 0 THEN TRUE
            ELSE FALSE
        END AS is_negative_leistung
    FROM raw_data
    WHERE DATE(Timestamp) = (SELECT last_date FROM max_date)
)

SELECT
    hour_of_day,
    Kraftwerk,
    is_peak_load,
    is_negative_leistung,
    raw_leistung AS leistung
FROM last_day_data
WHERE raw_leistung IS NOT NULL  -- Only show rows with valid Leistung
ORDER BY hour_of_day, Kraftwerk
    );
  