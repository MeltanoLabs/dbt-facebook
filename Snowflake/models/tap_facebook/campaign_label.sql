{{
   config(
     materialized='table'
   )
}}
 
SELECT ACCOUNT_ID,
       AD_LABEL_ID,
       ADLABELS,
       CREATED_TIME,
       ID,
       PROMOTED_OBJECT,
       SOURCE_CAMPAIGN_ID,
       UPDATED_TIME
FROM {{ source('tap_facebook', 'campaign_label') }} as meltano_campaign_label
