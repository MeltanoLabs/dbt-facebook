{{
   config(
     materialized='table'
   )
}}

 
SELECT ACCOUNT_ID,
       ID as ADSET_ID,
       TO_TIMESTAMP_NTZ(CREATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as AD_SET_CREATED_TIME,
       NAME,
       TO_TIMESTAMP_NTZ(UPDATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as AD_SET_UPDATED_TIME

FROM {{ source('tap_facebook', 'adsets') }} as pacing_type
