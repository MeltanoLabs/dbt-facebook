SELECT
    account_id,
    adlabels,
    TO_TIMESTAMP_NTZ(
        created_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM'
    ) AS campaign_created_time,
    id,
    TO_TIMESTAMP_NTZ(
        updated_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM'
    ) AS campaign_updated_time,
    _sdc_batched_at

FROM {{ source('tap_facebook', 'campaigns') }}
