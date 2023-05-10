{{
   config(
     materialized='view'
   )
}}

{% set json_column_query %}
select distinct json.key as column_name

FROM {{ source('tap_facebook', 'ads')}},

lateral flatten(input=>CREATIVE) json
{% endset %}
 
{% set creative_results = run_query(json_column_query) %}

{% if execute %}
{# Return the first column #}
{% set creative_list = creative_results.columns[0].values() %}
{% else %}
{% set creative_list = [] %}
{% endif %}

{% set json_column_query %}
select distinct json.key as column_name

FROM (SELECT REPLACE(ARRAY_TO_STRING(ARRAY_SLICE(PARSE_JSON(TRACKING_SPECS), 1, 2), ''), '.', '_') as TRACKING_COLUMNS FROM {{ source('tap_facebook', 'ads') }}),

lateral flatten(input=>PARSE_JSON(TRACKING_COLUMNS)) json
{% endset %}
 
{% set tracking_results = run_query(json_column_query) %}

{% if execute %}
{# Return the first column #}
{% set tracking_list = tracking_results.columns[0].values() %}
{% else %}
{% set tracking_list = [] %}
{% endif %}
 

SELECT AD_ID,
       AD_UPDATED_TIME,
       APPLICATION,
       "TRACKING_COLUMNS_conversion_id" as CONVERSION_ID,
       CREATIVE,
       DATASET,
       EVENT,
       EVENT_TYPE,
       "TRACKING_COLUMNS_fb_pixel" as FB_PIXEL,
       FB_PIXEL_EVENT,
       LEADGEN,
       OBJECT,
       OFFER,
       OFFSITE_PIXEL,
       "TRACKING_COLUMNS_page" as PAGE,
       "TRACKING_COLUMNS_post" as POST,
       QUESTION,
       RESPONSE,
       SUBTYPE,
       "TRACKING_COLUMNS_action_type" as ACTION_TYPE,
       EVENT_CREATOR,
       OBJECT_DOMAIN,
       OFFER_CREATOR,
       PAGE_PARENT,
       POST_OBJECT_WALL,
       "TRACKING_COLUMNS_post_wall" as POST_WALL,
       POST_OBJECT,
       QUESTION_CREATOR,
       _SDC_BATCHED_AT

FROM (SELECT AD_ID,
             AD_UPDATED_TIME,
             APPLICATION,
             CREATIVE
             DATASET,
             EVENT,
             EVENT_CREATOR,
             EVENT_TYPE,
             FB_PIXEL_EVENT,
             LEADGEN,
             OBJECT,
             OBJECT_DOMAIN,
             OFFER,
             OFFER_CREATOR,
             OFFSITE_PIXEL,
             PAGE_PARENT,
             POST_OBJECT,
             POST_OBJECT_WALL,
             QUESTION,
             QUESTION_CREATOR,
             RESPONSE,
             SUBTYPE,

       {% for column_name in tracking_list %}
       PARSE_JSON(TRACKING_COLUMNS):{{column_name}}::varchar as "TRACKING_COLUMNS_{{column_name}}"{%- if not loop.last %},{% endif -%}
       {% endfor %},

       CREATIVE,
       _SDC_BATCHED_AT

FROM (SELECT ID as AD_ID,
             APPLICATION,
             DATASET,
             EVENT,
             EVENT_CREATOR,
             EVENT_TYPE,
             FB_PIXEL_EVENT,
             LEADGEN,
             OBJECT,
             OBJECT_DOMAIN,
             OFFER,
             OFFER_CREATOR,
             OFFSITE_PIXEL,
             PAGE_PARENT,
             POST_OBJECT,
             POST_OBJECT_WALL,
             QUESTION,
             QUESTION_CREATOR,
             RESPONSE,
             SUBTYPE,
             REPLACE(ARRAY_TO_STRING(ARRAY_SLICE(PARSE_JSON(TRACKING_SPECS), 1, 2), ''), '.', '_') as TRACKING_COLUMNS,
       
             TO_TIMESTAMP_NTZ(UPDATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as AD_UPDATED_TIME,

             {% for column_name in creative_list %}
             CREATIVE:{{column_name}}::varchar as "CREATIVE"{%- if not loop.last %},{% endif -%}
             {% endfor %},
             _SDC_BATCHED_AT

FROM {{ source('tap_facebook', 'ads') }} as meltano_ad_tracking))
 
