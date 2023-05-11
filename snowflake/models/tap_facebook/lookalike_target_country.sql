{{
   config(
     materialized='view'
   )
}}
 
SELECT ID AS CUSTOM_AUDIENCE_ID,
       CAST(TIME_UPDATED as datetime) as CUSTOM_AUDIENCE_UPDATED_TIME,
        --COUNTRY
       _SDC_BATCHED_AT

FROM {{ source('tap_facebook', 'customaudiences') }} as meltano_custom_audiences
 