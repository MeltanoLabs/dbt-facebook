{{
   config(
     materialized='table'
   )
}}
 
SELECT ACCOUNT_ID,
       ID,
      /*TODO: Add 'Creatives' stream and pull data to use for this model, columns needed are found here: https://docs.google.com/spreadsheets/d/1v5lJTdeJrfiYoVx_ffgYlZiXGid5YsE1K_9eBX8-Wqg/edit#gid=0*/
FROM {{ source('tap_facebook', 'creatives') }} as meltano_creative_history
