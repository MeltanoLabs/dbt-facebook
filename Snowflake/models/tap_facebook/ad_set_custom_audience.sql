{{
   config(
     materialized='table'
   )
}}
 
SELECT ADSET_ID,
       UPDATED_TIME,
       /*
        TODO: ADD missing columns: CUSTOM_AUDIENCE_ID, IS_EXCLUDED
       */
FROM {{ source('tap_facebook', 'adsets') }} as meltano_ad_set_custom_audience
