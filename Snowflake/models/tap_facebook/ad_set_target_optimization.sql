{{
   config(
     materialized='table'
   )
}}
 
SELECT ID,
       UPDATED_TIME,
       INDEX,
       _SDC_BATCHED_AT
       /* TODO: Add missing columns 'Value', 'Key' */
FROM {{ source('tap_facebook', 'adsets') }} as meltano_ad_set_target_optimization
