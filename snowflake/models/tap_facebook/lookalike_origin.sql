SELECT
    id AS custom_audience_id,
    CAST(time_updated AS datetime) AS custom_audience_updated_time,
    index - 1 AS index,
    name,
    subtype AS type,
    --ORIGIN_CUSTOM_AUDIENCE_ID
    _sdc_batched_at

FROM {{ source('tap_facebook', 'customaudiences') }},
    LATERAL SPLIT_TO_TABLE(input => CAST(id AS varchar), '|')
