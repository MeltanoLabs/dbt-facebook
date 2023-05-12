SELECT id as ad_set_id,
       TO_TIMESTAMP_NTZ(updated_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as ad_set_updated_time,
       index-1 as index,
       ARRAY_TO_STRING(PARSE_JSON(pacing_type), '|') as name,
       _sdc_batched_at

FROM {{ source('tap_facebook', 'adsets') }} as meltano_pacing_type,
lateral split_to_table(input=>ARRAY_TO_STRING(PARSE_JSON(pacing_type), '|'), '|') PACING_TYPE_COLUMN
