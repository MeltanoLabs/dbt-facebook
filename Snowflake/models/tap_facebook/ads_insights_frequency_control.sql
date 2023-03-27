{{
   config(
     materialized='table'
   )
}}
 
SELECT ADSET_ID,
       UPDATED_TIME,
       INDEX,
       EVENT,
      /*TODO: Add Columns: 'Max_Frequency' and 'Interval'*/
FROM {{ source('tap_facebook', 'adsinsights') }} as meltano_frequency_control
