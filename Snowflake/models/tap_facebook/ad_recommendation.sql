{{
   config(
     materialized='view'
   )
}}

SELECT ID as AD_ID,
       TO_TIMESTAMP_NTZ(UPDATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as AD_UPDATED_TIME,
       PARSE_JSON(ARRAY_TO_STRING(PARSE_JSON(RECOMMENDATIONS), '')):code::number as CODE,
       PARSE_JSON(ARRAY_TO_STRING(PARSE_JSON(RECOMMENDATIONS), '')):blame_field::varchar as BLAME_FIELD,
       PARSE_JSON(ARRAY_TO_STRING(PARSE_JSON(RECOMMENDATIONS), '')):confidence::varchar as CONFIDENCE,
       PARSE_JSON(ARRAY_TO_STRING(PARSE_JSON(RECOMMENDATIONS), '')):importance::varchar as IMPORTANCE,
       PARSE_JSON(ARRAY_TO_STRING(PARSE_JSON(RECOMMENDATIONS), '')):message::varchar as MESSAGE,
       PARSE_JSON(ARRAY_TO_STRING(PARSE_JSON(RECOMMENDATIONS), '')):title::varchar as TITLE,
       RECOMMENDATION_DATA,
       _SDC_BATCHED_AT

FROM {{ source('tap_facebook', 'ads') }} as meltano_ad_recommendation
