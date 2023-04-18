{{
   config(
     materialized='table'
   )
}}
 
SELECT ACCOUNT_ID,
       ADLABELS,
       TO_TIMESTAMP_NTZ(CREATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as CREATED_TIME,
       ERROR_CODE,
       ERROR_MESSAGE,
       ERROR_SUMMARY,
       ERROR_TYPE,
       ID,
       INDEX,
       LEVEL,
       PROMOTED_OBJECT,
       TO_TIMESTAMP_NTZ(UPDATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as UPDATED_TIME
FROM {{ source('tap_facebook', 'campaigns') }} as meltano_ad_campaign_issues_info
