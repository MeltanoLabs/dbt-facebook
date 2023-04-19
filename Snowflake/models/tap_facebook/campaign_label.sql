{{
   config(
     materialized='table'
   )
}}
 
SELECT ACCOUNT_ID,
       ADLABELS,
       TO_TIMESTAMP_NTZ(CREATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as CAMPAIGN_CREATED_TIME,
       ID,
       TO_TIMESTAMP_NTZ(UPDATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as CAMPAIGN_UPDATED_TIME
       
FROM {{ source('tap_facebook', 'campaigns') }} as campaign_label
