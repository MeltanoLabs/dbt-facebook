{{
   config(
     materialized='table'
   )
}}
 
SELECT ACCOUNT_ID,
       ADLABELS,
       CREATED_TIME,
       ERROR_CODE,
       ERROR_MESSAGE,
       ERROR_SUMMARY,
       ERROR_TYPE,
       ID,
       INDEX,
       LEVEL,
       PROMOTED_OBJECT,
       UPDATED_TIME
FROM {{ source('tap_facebook', 'ad_campaign_issues_info') }} as meltano_ad_campaign_issues_info
