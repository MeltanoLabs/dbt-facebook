SELECT
    id AS custom_audience_id,
    CAST(time_updated AS datetime) AS custom_audience_updated_time,
    --COUNTRY
    _sdc_batched_at

FROM {{ source('tap_facebook', 'customaudiences') }}
