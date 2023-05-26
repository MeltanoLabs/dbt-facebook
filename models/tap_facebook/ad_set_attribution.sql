SELECT
    id AS ad_set_id,
    TO_TIMESTAMP_NTZ(
        updated_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM'
    ) AS ad_set_updated_time,
    index - 1 AS index,
    PARSE_JSON(value):event_type::varchar AS event_type,
    PARSE_JSON(value):window_days::number AS window_days,
    _sdc_batched_at

FROM {{ source('tap_facebook', 'adsets') }},
    LATERAL SPLIT_TO_TABLE(
        input => ARRAY_TO_STRING(PARSE_JSON(attribution_spec), '|'), '|'
    )
