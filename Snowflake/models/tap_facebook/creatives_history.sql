{{
   config(
     materialized='table'
   )
}}

{% set json_column_query %}
select distinct json.key as column_name

FROM {{ source('tap_facebook', 'creatives')}},

lateral flatten(input=>PARSE_JSON(OBJECT_STORY_SPEC)) json
{% endset %}
 
{% set object_story_results = run_query(json_column_query) %}

{% if execute %}
{# Return the first column #}
{% set object_story_list = object_story_results.columns[0].values() %}
{% else %}
{% set object_story_list = [] %}
{% endif %}

{% set json_column_query %}
select distinct json.key as column_name

FROM {{ source('tap_facebook', 'creatives')}},

lateral flatten(input=>PARSE_JSON(DEGREES_OF_FREEDOM_SPEC)) json
{% endset %}
 
{% set degrees_of_freedom_spec_results = run_query(json_column_query) %}

{% if execute %}
{# Return the first column #}
{% set degrees_of_freedom_list = degrees_of_freedom_spec_results.columns[0].values() %}
{% else %}
{% set degrees_of_freedom_list = [] %}
{% endif %}

{% set json_column_query %}
select distinct json.key as column_name

FROM {{ source('tap_facebook', 'creatives')}},

lateral flatten(input=>PARSE_JSON(ASSET_FEED_SPEC)) json
{% endset %}
 
{% set asset_feed_results = run_query(json_column_query) %}

{% if execute %}
{# Return the first column #}
{% set asset_feed_list = asset_feed_results.columns[0].values() %}
{% else %}
{% set asset_feed_list = [] %}
{% endif %}
 
SELECT ACCOUNT_ID,
       ID,
       ACTOR_ID,
       APPLINK_TREATMENT,
       ASSET_FEED_SPEC,
       AUTHORIZATION_CATEGORY,
       BODY,
       BRANDED_CONTENT_SPONSOR_PAGE_ID,
       BUNDLE_FOLDER_ID,
       CALL_TO_ACTION_TYPE,
       CATEGORIZATION_CRITERIA,
       CATEGORY_MEDIA_SOURCE,
       DEGREES_OF_FREEDOM_SPEC,
       DESTINATION_SET_ID,
       DYNAMIC_AD_VOICE,
       EFFECTIVE_AUTHORIZATION_CATEGORY,
       EFFECTIVE_INSTAGRAM_MEDIA_ID,
       EFFECTIVE_INSTAGRAM_STORY_ID,
       EFFECTIVE_OBJECT_STORY_ID, 
       ENABLE_DIRECT_INSTALL,
       IMAGE_HASH,
       IMAGE_URL,
       INSTAGRAM_ACTOR_ID,
       INSTAGRAM_PERMALINK_URL,
       INSTAGRAM_STORY_ID,
       LINK_DESTINATION_DISPLAY_URL,
       LINK_OG_ID,
       LINK_URL,
       MESSENGER_SPONSORED_MESSAGE,
       NAME,
       OBJECT_ID,
       OBJECT_STORE_URL,
       OBJECT_STORY_ID,
       OBJECT_STORY_SPEC,
       OBJECT_TYPE,
       OBJECT_URL,
       PAGE_LINK,
       PAGE_MESSAGE,
       PLACE_PAGE_SET_ID,
       PLATFORM_CUSTOMIZATIONS,
       PLAYABLE_ASSET_ID,
       SOURCE_INSTAGRAM_MEDIA_ID,
       STATUS,
       TEMPLATE_URL,
       THUMBNAIL_ID,
       THUMBNAIL_URL,
       TITLE,
       URL_TAGS,
       USE_PAGE_ACTOR_OVERRIDE,
       VIDEO_ID,
       _SDC_BATCHED_AT,


{% for column_name in object_story_list %}
PARSE_JSON(OBJECT_STORY_SPEC):{{column_name}}::varchar as "OBJECT_STORY_SPEC_{{column_name}}"{%- if not loop.last %},{% endif -%}
{% endfor %},

{% for column_name in degrees_of_freedom_list %}
PARSE_JSON(DEGREES_OF_FREEDOM_SPEC):{{column_name}}::varchar as "DEGREES_OF_FREEDOM_SPEC_{{column_name}}"{%- if not loop.last %},{% endif -%}
{% endfor %},

{% for column_name in asset_feed_list %}
PARSE_JSON(ASSET_FEED_SPEC):{{column_name}}::varchar as "ASSET_FEED_SPEC_{{column_name}}"{%- if not loop.last %},{% endif -%}
{% endfor %}

       
FROM {{ source('tap_facebook', 'creatives') }} as creative_history
