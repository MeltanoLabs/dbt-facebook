{{
   config(
     materialized='table'
   )
}}
 
SELECT ACCOUNT_ID,
       ADSET_ID,
       APPLICATION,
       CAMPAIGN_ID,
       CONVERSION_SPECS,
       CREATED_TIME,
       CREATIVE,
       DATASET,
       EVENT,
       EVENT_CREATOR,
       EVENT_TYPE,
       FB_PIXEL,
       FB_PIXEL_EVENT,
       ID,
       INDEX,
       LEADGEN,
       NAME,
       OBJECT,
       OBJECT_DOMAIN,
       OFFER,
       OFFER_CREATOR,
       OFFSITE_PIXEL,
       PAGE,
       PAGE_PARENT,
       POST,
       POST_OBJECT,
       POST_OBJECT_WALL,
       POST_WALL,
       QUESTION,
       QUESTION_CREATOR,
       RESPONSE,
       SUBTYPE,
       TRACKING_SPECS,
       UPDATED_TIME

       /*TODO: Add columns 'Action_type' and 'Conversion_id'*/

FROM {{ source('tap_facebook', 'ads') }} as meltano_ad_conversion
