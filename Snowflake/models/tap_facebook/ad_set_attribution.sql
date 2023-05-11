SELECT ID as AD_SET_ID,
       TO_TIMESTAMP_NTZ(UPDATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as AD_SET_UPDATED_TIME,
       INDEX-1 as INDEX,
       PARSE_JSON(VALUE):event_type::varchar as EVENT_TYPE,
       PARSE_JSON(VALUE):window_days::number as WINDOW_DAYS,
       _SDC_BATCHED_AT

FROM {{ source('tap_facebook', 'adsets') }} as ad_set_attribution,
lateral split_to_table(input=>ARRAY_TO_STRING(PARSE_JSON(ATTRIBUTION_SPEC), '|'), '|') ATTRIBUTION_SPEC_COLUMN