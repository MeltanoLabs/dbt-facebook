SELECT id AS custom_audience_id,
       CAST(time_updated as datetime) as custom_audience_updated_time,
       index-1 as index,
       name,
       subtype as type,
       --ORIGIN_CUSTOM_AUDIENCE_ID
       _sdc_batched_at

FROM {{ source('tap_facebook', 'customaudiences') }},
lateral split_to_table(input=>CAST(id as varchar), '|') LOOKALIKE_COLUMN