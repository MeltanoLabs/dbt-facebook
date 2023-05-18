SELECT
    id AS ad_set_id,
    TO_TIMESTAMP_NTZ(
        updated_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM'
    ) AS ad_set_updated_time,
    index - 1 AS index,
    MINUTE(
        TO_TIMESTAMP_NTZ(start_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM')
    ) AS start_minute,
    MINUTE(
        TO_TIMESTAMP_NTZ(end_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM')
    ) AS end_minute,
    --TIMEZONE_TYPE
    _sdc_batched_at

FROM {{ source('tap_facebook', 'adsets') }},
    LATERAL SPLIT_TO_TABLE(input => CAST(id AS varchar), '|')
