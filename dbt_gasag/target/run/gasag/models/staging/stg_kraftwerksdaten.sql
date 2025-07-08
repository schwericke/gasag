
  
    

    create or replace table `gasag-465208`.`analysis`.`stg_kraftwerksdaten`
      
    
    

    OPTIONS()
    as (
      WITH raw_data AS (
    SELECT
        Kraftwerk,
        -- Parse German date format and convert to TIMESTAMP
        PARSE_TIMESTAMP('%d.%m.%Y %H:%M', Timestamp) as Timestamp,
        Zaehlerstand,
        -- Flag decreasing meter readings
        CASE
            WHEN LAG(Zaehlerstand) OVER (PARTITION BY Kraftwerk ORDER BY PARSE_TIMESTAMP('%d.%m.%Y %H:%M', Timestamp))
            > Zaehlerstand
            THEN TRUE
            ELSE FALSE
        END AS is_decreasing_zaehlerstand
    FROM `gasag-465208`.`raw`.`kraftwerksdaten_raw`
),

max_date AS (
    SELECT MAX(DATE(PARSE_TIMESTAMP('%d.%m.%Y %H:%M', Timestamp))) as last_date
    FROM `gasag-465208`.`raw`.`kraftwerksdaten_raw`
),

cleaned_data AS (
    SELECT
        Kraftwerk,
        Timestamp,
        Zaehlerstand,
        is_decreasing_zaehlerstand,
        -- Flag last day dynamically
        CASE
            WHEN DATE(Timestamp) = (SELECT last_date FROM max_date) THEN TRUE
            ELSE FALSE
        END AS is_last_day,
        -- Calculate Leistung (NULL for decreasing readings or first timestamp)
        CASE
            WHEN is_decreasing_zaehlerstand THEN NULL
            ELSE Zaehlerstand - LAG(Zaehlerstand) OVER (PARTITION BY Kraftwerk ORDER BY Timestamp)
        END AS leistung
    FROM raw_data
)

SELECT
    Kraftwerk,
    Timestamp,
    EXTRACT(HOUR FROM Timestamp) AS hour_of_day,
    Zaehlerstand,
    is_decreasing_zaehlerstand,
    is_last_day,
    leistung,
    -- Flag peak loads > 50MW (only for non-NULL Leistung)
    CASE
        WHEN leistung IS NOT NULL AND leistung > 50 THEN TRUE
        ELSE FALSE
    END AS is_peak_load,
    -- Flag for forecast data (will be populated by ARIMA model)
    FALSE AS is_forecast,
    -- Prognose will be populated by ARIMA model
    NULL AS prognose
FROM cleaned_data
WHERE Timestamp > TIMESTAMP('2024-01-01 00:00:00')
ORDER BY Kraftwerk, Timestamp
    );
  