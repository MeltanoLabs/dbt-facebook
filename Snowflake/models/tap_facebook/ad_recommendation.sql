{{
   config(
     materialized='view'
   )
}}
 
SELECT ACCOUNT_ID,
       ADLABELS,
       BLAME_FIELD,
       CODE,
       CONFIDENCE,
       TO_TIMESTAMP_NTZ(CREATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as CREATED_TIME,
       ID,
       IMPORTANCE,
       0 as INDEX,
       MESSAGE,
       PROMOTED_OBJECT,
       RECOMMENDATION_DATA,
       TITLE,
       TO_TIMESTAMP_NTZ(UPDATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as UPDATED_TIME,
       _SDC_BATCHED_AT

FROM {{ source('tap_facebook', 'ads') }} as meltano_ad_recommendation
