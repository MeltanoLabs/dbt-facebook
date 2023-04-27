{{
   config(
     materialized='table'
   )
}}
 
SELECT ACCOUNT_ID,
      
      /*TODO: Add Columns: COUNTRY_CODE, MIN_REACH_LIMITS, MAX_DAYS_TO_FINISH, MIN_CAMPAIGN_DURATION, MAX_CAMPAIGN_DURATION */
      _SDC_BATCHED_AT
FROM {{ source('tap_facebook', 'adsinsights') }} as reach_frequency
