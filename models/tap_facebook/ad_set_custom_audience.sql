SELECT
    id,
    TO_TIMESTAMP_NTZ(
        updated_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM'
    ) AS updated_time,
    _sdc_batched_at
--CUSTOM_AUDIENCE_ID, 
--IS_EXCLUDED

FROM {{ source('tap_facebook', 'adsets') }}
