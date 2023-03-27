{{
   config(
     materialized='table'
   )
}}
 
SELECT ACCOUNT_ID,
       ADLABELS,
       ADSET_ID,
       BID_AMOUNT,
       BID_INFO,
       BID_TYPE,
       CAMPAIGN_ID,
       CONFIGURED_STATUS,
       CONVERSION_DOMAIN,
       CONVERSION_SPECS,
       CREATED_TIME,
       CREATIVE,
       EFFECTIVE_STATUS,
       ID,
       LAST_UPDATED_BY_APP_ID,
       NAME,
       RECOMMENDATIONS,
       SOURCE_AD_ID,
       STATUS,
       TRACKING_SPECS,
       UPDATED_TIME
FROM {{ source('tap_facebook', 'ads') }} as meltano_ad_history
