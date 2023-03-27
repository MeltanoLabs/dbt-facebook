{{
   config(
     materialized='table'
   )
}}
 
SELECT ADSET_ID,
       UPDATED_TIME,
       INDEX,
       /* TODO: Add missing columns 'Value', 'Key' */
FROM {{ source('tap_facebook', 'adsets') }} as meltano_ad_set_target_optimization
