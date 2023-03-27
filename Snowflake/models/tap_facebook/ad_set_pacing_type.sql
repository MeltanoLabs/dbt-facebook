{{
   config(
     materialized='table'
   )
}}
 
SELECT ADSET_ID,
       UPDATED_TIME,
       INDEX,
       NAME
FROM {{ source('tap_facebook', 'adsets') }} as meltano_ad_set_pacing_type
