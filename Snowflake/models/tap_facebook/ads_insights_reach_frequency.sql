{{
   config(
     materialized='table'
   )
}}
 
SELECT ACCOUNT_ID
      
      /*TODO: Add Columns: COUNTRY_CODE, MIN_REACH_LIMITS, MAX_DAYS_TO_FINISH, MIN_CAMPAIGN_DURATION, MAX_CAMPAIGN_DURATION */
FROM {{ source('tap_facebook', 'adsinsights') }} as meltano_reach_frequency
