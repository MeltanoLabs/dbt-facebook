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
       UPDATED_TIME,
       CAMPAIGN_ID,
       /*TODO: Add column: CAMPAIGN_UPDATED_TIME */
FROM {{ source('tap_facebook', 'campaigns') }} as meltano_campaign_label
