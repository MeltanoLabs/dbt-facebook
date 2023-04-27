{{
   config(
     materialized='table'
   )
}}

 
SELECT ID as ADSET_ID,
       TO_TIMESTAMP_NTZ(UPDATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as AD_SET_UPDATED_TIME,
       /* TODO: Find and add column 'INDEX'*/
       NAME,
       _SDC_BATCHED_AT
       

FROM {{ source('tap_facebook', 'adsets') }} as pacing_type
