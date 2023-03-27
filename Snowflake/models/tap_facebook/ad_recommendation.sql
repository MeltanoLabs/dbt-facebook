{{
   config(
     materialized='table'
   )
}}
 
SELECT ACCOUNT_ID,
       ADLABELS,
       BLAME_FIELD,
       CODE,
       CONFIDENCE,
       CREATED_TIME,
       ID,
       IMPORTANCE,
       INDEX,
       MESSAGE,
       PROMOTED_OBJECT,
       RECOMMENDATION_DATA,
       TITLE,
       UPDATED_TIME
FROM {{ source('tap_facebook', 'ads') }} as meltano_ad_recommendation
