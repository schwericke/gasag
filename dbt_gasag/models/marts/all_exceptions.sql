WITH raw_data AS (
    SELECT
        Kraftwerk,
        -- Parse German date format and convert to TIMESTAMP
        PARSE_TIMESTAMP('%d.%m.%Y %H:%M', Timestamp) as Timestamp,
        Zaehlerstand,
        -- Calculate raw Leistung (including negative values)
        Zaehlerstand - LAG(Zaehlerstand) OVER (PARTITION BY Kraftwerk ORDER BY PARSE_TIMESTAMP('%d.%m.%Y %H:%M', Timestamp)) AS raw_leistung
    FROM {{ source('raw', 'kraftwerksdaten_raw') }}
),

staging_data AS (
    SELECT
        Kraftwerk,
        Timestamp,
        leistung,
        is_peak_load
    FROM {{ ref('stg_kraftwerksdaten') }}
),

all_exceptions AS (
    -- Peak loads from cleaned staging data
    SELECT
        s.Kraftwerk,
        s.Timestamp,
        s.leistung,
        TRUE AS is_peak_load,
        FALSE AS is_negative_leistung
    FROM staging_data s
    WHERE s.is_peak_load = TRUE AND s.leistung IS NOT NULL

    UNION ALL

    -- Negative spikes from raw data
    SELECT
        r.Kraftwerk,
        r.Timestamp,
        r.raw_leistung AS leistung,
        FALSE AS is_peak_load,
        TRUE AS is_negative_leistung
    FROM raw_data r
    WHERE r.raw_leistung < 0 AND r.raw_leistung IS NOT NULL
)

SELECT
    Kraftwerk,
    Timestamp,
    EXTRACT(HOUR FROM Timestamp) AS hour_of_day,
    EXTRACT(DATE FROM Timestamp) AS date,
    is_peak_load,
    is_negative_leistung,
    leistung
FROM all_exceptions
ORDER BY Timestamp DESC, Kraftwerk
