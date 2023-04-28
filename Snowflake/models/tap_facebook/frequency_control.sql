{{
   config(
     materialized='view'
   )
}}
 
SELECT ID as AD_SET_ID,
       CREATED_TIME as AD_SET_CREATED_TIME,
       UPDATED_TIME as AD_SET_UPDATED_TIME,
       0 as INDEX,
       1 as MAX_FREQUENCY,
       7 as INTERVAL_DAYS,
       BILLING_EVENT as EVENT,
       _SDC_BATCHED_AT
       
FROM {{ source('tap_facebook', 'adsets') }} as FREQUENCY_CONTROL
