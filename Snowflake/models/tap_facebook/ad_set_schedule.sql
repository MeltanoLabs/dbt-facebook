{{
   config(
     materialized='table'
   )
}}
 
SELECT ADSET_ID,
       UPDATED_TIME,
       INDEX,
       START_TIME, /* TODO: Find Start_minute column and replace here */
       END_TIME, /* TODO: Find End_minute column and replace here */
       
      /* TODO: Add Timezone_type column */

FROM {{ source('tap_facebook', 'adsets') }} as meltano_ad_set_pacing_type
