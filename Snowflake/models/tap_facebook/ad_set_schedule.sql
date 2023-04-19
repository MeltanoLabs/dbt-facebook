{{
   config(
     materialized='table'
   )
}}
 
SELECT ID,
       UPDATED_TIME,
       INDEX,
       MINUTE(TO_TIMESTAMP_NTZ(START_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM')) as START_MINUTE,
       MINUTE(TO_TIMESTAMP_NTZ(END_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM')) as END_MINUTE
       
      /* TODO: Add Timezone_type column */

FROM {{ source('tap_facebook', 'adsets') }} as meltano_ad_set_pacing_type
