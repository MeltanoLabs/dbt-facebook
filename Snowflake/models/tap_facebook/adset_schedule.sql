{{
   config(
     materialized='view'
   )
}}
 
SELECT ID as ADSET_ID,
       TO_TIMESTAMP_NTZ(CREATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as CREATED_TIME,
       0 as INDEX,
       TO_TIMESTAMP_NTZ(UPDATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as UPDATED_TIME,
       TO_TIMESTAMP_NTZ(START_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as START_TIME, 
       TO_TIMESTAMP_NTZ(END_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as END_TIME,
       MINUTE(TO_TIMESTAMP_NTZ(START_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM')) as START_MINUTE,
       MINUTE(TO_TIMESTAMP_NTZ(END_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM')) as END_MINUTE,
       --TIMEZONE_TYPE
       _SDC_BATCHED_AT

FROM {{ source('tap_facebook', 'adsets') }} as meltano_ad_set_schedule
