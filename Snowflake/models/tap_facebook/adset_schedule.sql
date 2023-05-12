SELECT id as adset_id,
       TO_TIMESTAMP_NTZ(created_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as created_time,
       NULL as index, --add data for index
       TO_TIMESTAMP_NTZ(updated_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as updated_time,
       TO_TIMESTAMP_NTZ(start_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as start_time, 
       TO_TIMESTAMP_NTZ(end_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as end_time,
       MINUTE(TO_TIMESTAMP_NTZ(start_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM')) as start_minute,
       MINUTE(TO_TIMESTAMP_NTZ(end_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM')) as end_minute,
       --TIMEZONE_TYPE
       _sdc_batched_at

FROM {{ source('tap_facebook', 'adsets') }}