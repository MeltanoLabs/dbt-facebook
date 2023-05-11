{{
   config(
     materialized='view'
   )
}}
 
SELECT ID as AD_ID,
       TO_TIMESTAMP_NTZ(UPDATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as AD_UPDATED_TIME,
       INDEX-1 as INDEX,
       VALUE,
       APPLICATION,
       PARSE_JSON(CREATIVE):id::number as CREATIVE,
       DATASET,
       CAST(PARSE_JSON(value):"event" as variant) as EVENT,
       CAST(PARSE_JSON(value):"event_type" as variant) as EVENT_TYPE,
       CAST(PARSE_JSON(value):"fb_pixel" as variant) as FB_PIXEL,
       CAST(PARSE_JSON(value):"fb_pixel_event" as variant) as FB_PIXEL_EVENT,
       CAST(PARSE_JSON(value):"leadgen" as variant) as LEADGEN,
       CAST(PARSE_JSON(value):"object" as variant) as OBJECT,
       CAST(PARSE_JSON(value):"offer" as variant) as OFFER,
       CAST(PARSE_JSON(value):"offsite_pixel" as variant) as OFFSITE_PIXEL,
       CAST(PARSE_JSON(value):"page" as variant) as PAGE,
       CAST(PARSE_JSON(value):"post" as variant) as POST,
       CAST(PARSE_JSON(value):"question" as variant) as QUESTION,
       CAST(PARSE_JSON(value):"response" as variant) as RESPONSE,
       CAST(PARSE_JSON(value):"subtype" as variant) as SUBTYPE,
       CAST(PARSE_JSON(value):"action.type" as variant) as ACTION_TYPE,
       CAST(PARSE_JSON(value):"event_creator" as variant) as EVENT_CREATOR,
       CAST(PARSE_JSON(value):"object_domain" as variant) as OBJECT_DOMAIN,
       CAST(PARSE_JSON(value):"offer_creator" as variant) as OFFER_CREATOR,
       CAST(PARSE_JSON(value):"page_parent" as variant) as PAGE_PARENT,
       CAST(PARSE_JSON(value):"post_object_wall" as variant) as POST_OBJECT_WALL,
       CAST(PARSE_JSON(value):"post_wall" as variant) as POST_WALL,
       CAST(PARSE_JSON(value):"post_object" as variant) as POST_OBJECT,
       CAST(PARSE_JSON(value):"question_creator" as variant) as QUESTION_CREATOR,
       CAST(PARSE_JSON(value):"conversion_id" as variant) as CONVERSION_ID,
       _SDC_BATCHED_AT

FROM {{ source('tap_facebook', 'ads') }} as meltano_ad_conversion,
lateral split_to_table(input=>ARRAY_TO_STRING(PARSE_JSON(CONVERSION_SPECS), '|'), '|') CONVERSION_COLUMNS
