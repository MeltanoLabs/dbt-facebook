{{
   config(
     materialized='table'
   )
}}

 
SELECT ACCOUNT_ID,
       ID as ADSET_ID,
       CREATED_TIME as AD_SET_CREATED_TIME,
       NAME,
       UPDATED_TIME as AD_SET_UPDATED_TIME

FROM {{ source('tap_facebook', 'adsets') }} as pacing_type
