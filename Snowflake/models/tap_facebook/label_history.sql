SELECT
    id,
    TO_TIMESTAMP_NTZ(
        updated_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM'
    ) AS updated_time,
    account:account_id::number AS account_id,
    TO_TIMESTAMP_NTZ(
        created_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM'
    ) AS created_time,
    name,
    _sdc_batched_at

FROM {{ source('tap_facebook', 'adlabels') }}
