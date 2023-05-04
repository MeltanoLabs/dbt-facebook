{{
   config(
     materialized='view'
   )
}}
 
SELECT ID,
       ACCOUNT_ID,
       NAME

FROM {{ source('tap_facebook', 'customconversions') }} as meltano_custom_conversion_history
