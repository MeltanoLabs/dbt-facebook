SELECT id as ad_set_id,
       TO_TIMESTAMP_NTZ(updated_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') ad_set_updated_time,
       index-1 as index,
       --DAYS
       MINUTE(TO_TIMESTAMP_NTZ(start_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM')) as start_minute,
       MINUTE(TO_TIMESTAMP_NTZ(end_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM')) as end_minute,
       --TIMEZONE_TYPE
       _sdc_batched_at

FROM {{ source('tap_facebook', 'adsets') }},
lateral split_to_table(input=>CAST(id as varchar), '|') ADSET_COLUMN
