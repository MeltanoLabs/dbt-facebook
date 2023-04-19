{{
   config(
     materialized='table'
   )
}}
 
SELECT ID,
       TO_TIMESTAMP_NTZ(UPDATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as UPDATED_TIME
       /*
        TODO: ADD missing columns: CUSTOM_AUDIENCE_ID, IS_EXCLUDED
       */
FROM {{ source('tap_facebook', 'adsets') }} as meltano_ad_set_custom_audience
