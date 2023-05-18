SELECT
    id AS ad_set_id,
    TO_TIMESTAMP_NTZ(
        updated_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM'
    ) AS ad_set_updated_time,
    index - 1 AS index,
    ARRAY_TO_STRING(PARSE_JSON(pacing_type), '|') AS name,
    _sdc_batched_at

FROM {{ source('tap_facebook', 'adsets') }},
    LATERAL SPLIT_TO_TABLE(
        input => ARRAY_TO_STRING(PARSE_JSON(pacing_type), '|'), '|'
    )
