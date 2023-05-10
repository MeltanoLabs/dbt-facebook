{{
   config(
     materialized='view'
   )
}}
 
SELECT ID as AD_SET_ID,
       TO_TIMESTAMP_NTZ(UPDATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as AD_SET_UPDATED_TIME,
       NULL as INDEX, /* Add value for INDEX*/
       ARRAY_TO_STRING(PARSE_JSON(PACING_TYPE), '') as NAME,
       _SDC_BATCHED_AT


FROM {{ source('tap_facebook', 'adsets') }} as pacing_type
