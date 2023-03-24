{{
   config(
     materialized='table'
   )
}}
 
SELECT ACCOUNT_ID,
       ADLABELS,
       CREATED_TIME,
       EVENT_TYPE,
       ID,
       INDEX,
       PROMOTED_OBJECT,
       UPDATED_TIME,
       WINDOW_DAYS
FROM {{ source('tap_facebook', 'ad_set_attribution') }} as meltano_ad_set_attribution
