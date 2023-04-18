{{
   config(
     materialized='table'
   )
}}
 
SELECT ID,
       TO_TIMESTAMP_NTZ(CREATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as CREATED_TIME,
       ACCOUNT_ID,
       TO_TIMESTAMP_NTZ(UPDATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as UPDATED_TIME

FROM {{ source('tap_facebook', 'adlabels') }} as label_history
