{{
   config(
     materialized='view'
   )
}}
 
SELECT ID,
       ACCOUNT_ID,
       NAME,
       _SDC_BATCHED_AT

FROM {{ source('tap_facebook', 'customconversions') }} as meltano_custom_conversion_history
