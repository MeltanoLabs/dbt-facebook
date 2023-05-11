SELECT id as ad_set_id,
       TO_TIMESTAMP_NTZ(updated_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as ad_set_updated_time,
       index-1 as index,
       PARSE_JSON(value):event_type::varchar as event_type,
       PARSE_JSON(value):window_days::number as window_days,
       _sdc_batched_at

FROM {{ source('tap_facebook', 'adsets') }} as ad_set_attribution,
lateral split_to_table(input=>ARRAY_TO_STRING(PARSE_JSON(attribution_spec), '|'), '|') attribution_spec_column