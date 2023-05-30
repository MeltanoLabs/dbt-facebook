SELECT
    id AS ad_set_id,
    TO_TIMESTAMP_NTZ(
        updated_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM'
    ) AS ad_set_updated_time,
    index - 1 AS index,
    billing_event AS event,
    {{ var("frequency_control")["interval_days"] }} AS interval_days,
    {{ var("frequency_control")["max_frequency"] }} AS max_frequency,
    _sdc_batched_at

FROM {{ source('tap_facebook', 'adsets') }},
    LATERAL SPLIT_TO_TABLE(input => billing_event, '|')
