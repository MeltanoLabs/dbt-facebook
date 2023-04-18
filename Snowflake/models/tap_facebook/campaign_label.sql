{{
   config(
     materialized='table'
   )
}}
 
SELECT ACCOUNT_ID,
       AD_LABEL_ID,
       ADLABELS,
       TO_TIMESTAMP_NTZ(CREATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as CAMPAIGN_CREATED_TIME,
       ID,
       PROMOTED_OBJECT,
       SOURCE_CAMPAIGN_ID,
       TO_TIMESTAMP_NTZ(UPDATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as CAMPAIGN_UPDATED_TIME,
       CAMPAIGN_ID,
       /*TODO: Add column: CAMPAIGN_UPDATED_TIME */
FROM {{ source('tap_facebook', 'campaigns') }} as meltano_campaign_label
