{{
   config(
     materialized='view'
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
       0 as INDEX,
       LEVEL,
       PROMOTED_OBJECT,
       TO_TIMESTAMP_NTZ(UPDATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as UPDATED_TIME,
       _SDC_BATCHED_AT

FROM {{ source('tap_facebook', 'ads') }} as meltano_ad_group_issues_info
