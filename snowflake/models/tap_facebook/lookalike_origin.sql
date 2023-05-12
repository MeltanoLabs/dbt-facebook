SELECT id AS custom_audience_id,
       CAST(time_updated as datetime) as custom_audience_updated_time,
        NULL as index, --add data for INDEX
        name,
        --TYPE
        --ORIGIN_CUSTOM_AUDIENCE_ID
       _sdc_batched_at

FROM {{ source('tap_facebook', 'customaudiences') }}