{{
   config(
     materialized='view'
   )
}}
 
SELECT ID,
       TO_TIMESTAMP_NTZ(UPDATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as UPDATED_TIME,
       ACCOUNT:account_id::number as ACCOUNT_ID,
       TO_TIMESTAMP_NTZ(CREATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as CREATED_TIME,
       NAME,
       _SDC_BATCHED_AT       

FROM {{ source('tap_facebook', 'adlabels') }} as label_history
