SELECT id as ad_set_id,
       TO_TIMESTAMP_NTZ(updated_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as ad_set_updated_time,
       NULL as index, --Add data for INDEX
       ARRAY_TO_STRING(PARSE_JSON(pacing_type), '') as name,
       _sdc_batched_at


FROM {{ source('tap_facebook', 'adsets') }} as pacing_type