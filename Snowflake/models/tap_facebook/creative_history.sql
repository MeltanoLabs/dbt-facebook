{{
   config(
     materialized='view'
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
       AUTHORIZATION_CATEGORY,
       REPLACE(PARSE_JSON("OBJECT_STORY_SPEC_video_data"):title, '"')::varchar as BODY,
       BRANDED_CONTENT_SPONSOR_PAGE_ID,
       BUNDLE_FOLDER_ID,
       CALL_TO_ACTION_TYPE,
       CATEGORIZATION_CRITERIA,
       CATEGORY_MEDIA_SOURCE,
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
       OBJECT_TYPE,
       OBJECT_URL,
       PLACE_PAGE_SET_ID,
       PLAYABLE_ASSET_ID,
       PRODUCT_SET_ID,
       SOURCE_INSTAGRAM_MEDIA_ID,
       STATUS,
       TEMPLATE_URL,
       THUMBNAIL_ID,
       THUMBNAIL_URL,
       TITLE,
       USE_PAGE_ACTOR_OVERRIDE,
       VIDEO_ID,
       CAROUSEL_AD_LINK,
       REPLACE(PARSE_JSON(ARRAY_TO_STRING(ARRAY_SLICE(PARSE_JSON("OBJECT_STORY_SPEC_link_data"):child_attachments, 0, 1),'')):link, '"','') as PAGE_LINK,
       TEMPLATE_PAGE_LINK,
       URL_TAGS_ARRAY as URL_TAGS,
       OBJECT_STORY_LINK_DATA_CAPTION,
       REPLACE(PARSE_JSON("OBJECT_STORY_SPEC_link_data"):message, '"','')::varchar as OBJECT_STORY_LINK_DATA_MESSAGE,
       OBJECT_STORY_LINK_DATA_DESCRIPTION,
       REPLACE(PARSE_JSON(ARRAY_TO_STRING(ARRAY_SLICE(PARSE_JSON("OBJECT_STORY_SPEC_link_data"):child_attachments, 0, 1),'')):link, '"','') as OBJECT_STORY_LINK_DATA_LINK,
       PARSE_JSON("OBJECT_STORY_SPEC_link_data"):child_attachments as OBJECT_STORY_LINK_DATA_CHILD_ATTACHMENTS,
       OBJECT_STORY_LINK_DATA_APP_LINK_SPEC as OBJECT_STORY_LINK_DATA_APP_LINK_SPEC_ANDROID,
       OBJECT_STORY_LINK_DATA_APP_LINK_SPEC as OBJECT_STORY_LINK_DATA_APP_LINK_SPEC_IOS,
       OBJECT_STORY_LINK_DATA_APP_LINK_SPEC as OBJECT_STORY_LINK_DATA_APP_LINK_SPEC_IPAD,
       OBJECT_STORY_LINK_DATA_APP_LINK_SPEC as OBJECT_STORY_LINK_DATA_APP_LINK_SPEC_IPHONE,
       TEMPLATE_APP_LINK_SPEC_ANDROID,
       TEMPLATE_APP_LINK_SPEC_IOS,
       TEMPLATE_APP_LINK_SPEC_IPAD,
       TEMPLATE_APP_LINK_SPEC_IPHONE,
       TEMPLATE_CAPTION,
       TEMPLATE_CHILD_ATTACHMENTS,
       TEMPLATE_DESCRIPTION,
       TEMPLATE_LINK,
       TEMPLATE_MESSAGE,
       PAGE_MESSAGE,
       REPLACE(PARSE_JSON("OBJECT_STORY_SPEC_link_data"):call_to_action:value:link, '"','')::varchar as VIDEO_CALL_TO_ACTION_VALUE_LINK,
       PLATFORM_CUSTOMIZATIONS_INSTAGRAM_CAPTION_IDS,
       PLATFORM_CUSTOMIZATIONS_INSTAGRAM_IMAGE_HASH,
       PLATFORM_CUSTOMIZATIONS_INSTAGRAM_IMAGE_URL,
       PLATFORM_CUSTOMIZATIONS_INSTAGRAM_VIDEO_ID,
       TEMPLATE_URL_SPEC_ANDROID_APP_NAME,
       TEMPLATE_URL_SPEC_ANDROID_URL,
       TEMPLATE_URL_SPEC_ANDROID_PACKAGE,
       TEMPLATE_URL_SPEC_CONFIG_APP_ID,
       TEMPLATE_URL_SPEC_IOS_APP_NAME,
       TEMPLATE_URL_SPEC_IOS_APP_STORE_ID,
       TEMPLATE_URL_SPEC_IOS_URL,
       TEMPLATE_URL_SPEC_IPHONE_APP_NAME,
       TEMPLATE_URL_SPEC_IPHONE_APP_STORE_ID,
       TEMPLATE_URL_SPEC_IPHONE_URL,
       TEMPLATE_URL_SPEC_IPAD_APP_NAME,
       TEMPLATE_URL_SPEC_IPAD_APP_STORE_ID,
       TEMPLATE_URL_SPEC_IPAD_URL,
       TEMPLATE_URL_SPEC_WEB_SHOULD_FALLBACK,
       TEMPLATE_URL_SPEC_WEB_URL,
       TEMPLATE_URL_SPEC_WINDOWS_PHONE_APP_ID,
       TEMPLATE_URL_SPEC_WINDOWS_PHONE_APP_NAME,
       TEMPLATE_URL_SPEC_WINDOWS_PHONE_URL,
       PARSE_JSON("ASSET_FEED_SPEC_link_urls") as ASSET_FEED_SPEC_LINK_URLS,
       _SDC_BATCHED_AT

FROM (SELECT ACCOUNT_ID::number as ACCOUNT_ID,
             ID,
             ACTOR_ID::number as ACTOR_ID,
             APPLINK_TREATMENT,
             AUTHORIZATION_CATEGORY,
             BRANDED_CONTENT_SPONSOR_PAGE_ID::number as BRANDED_CONTENT_SPONSOR_PAGE_ID,
             BUNDLE_FOLDER_ID::number as BUNDLE_FOLDER_ID,
             CALL_TO_ACTION_TYPE,
             CATEGORIZATION_CRITERIA,
             CATEGORY_MEDIA_SOURCE,
             DESTINATION_SET_ID::number as DESTINATION_SET_ID,
             DYNAMIC_AD_VOICE,
             EFFECTIVE_AUTHORIZATION_CATEGORY,
             EFFECTIVE_INSTAGRAM_MEDIA_ID::number as EFFECTIVE_INSTAGRAM_MEDIA_ID,
             EFFECTIVE_INSTAGRAM_STORY_ID::number as EFFECTIVE_INSTAGRAM_STORY_ID,
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
             PLACE_PAGE_SET_ID,
             PLAYABLE_ASSET_ID,
             PRODUCT_SET_ID::number as PRODUCT_SET_ID,
             PLATFORM_CUSTOMIZATIONS,
             SOURCE_INSTAGRAM_MEDIA_ID,
             STATUS,
             TEMPLATE_URL,
             THUMBNAIL_ID,
             THUMBNAIL_URL,
             TITLE,
             USE_PAGE_ACTOR_OVERRIDE,
             VIDEO_ID,
             CAROUSEL_AD_LINK,
             TEMPLATE_PAGE_LINK,
             URL_TAGS,
             OBJECT_STORY_LINK_DATA_CAPTION,
             OBJECT_STORY_LINK_DATA_DESCRIPTION,
             TEMPLATE_APP_LINK_SPEC_ANDROID,
             TEMPLATE_APP_LINK_SPEC_IOS,
             TEMPLATE_APP_LINK_SPEC_IPAD,
             TEMPLATE_APP_LINK_SPEC_IPHONE,
             TEMPLATE_CAPTION,
             TEMPLATE_CHILD_ATTACHMENTS,
             TEMPLATE_DESCRIPTION,
             TEMPLATE_LINK,
             TEMPLATE_MESSAGE,
             PAGE_MESSAGE,
             PLATFORM_CUSTOMIZATIONS_INSTAGRAM_CAPTION_IDS,
             PLATFORM_CUSTOMIZATIONS_INSTAGRAM_IMAGE_HASH,
             PLATFORM_CUSTOMIZATIONS_INSTAGRAM_IMAGE_URL,
             PLATFORM_CUSTOMIZATIONS_INSTAGRAM_VIDEO_ID,
             OBJECT_STORY_LINK_DATA_DESCRIPTION as OBJECT_STORY_LINK_DATA_APP_LINK_SPEC,
             TEMPLATE_URL_SPEC_ANDROID_APP_NAME,
             TEMPLATE_URL_SPEC_ANDROID_URL,
             TEMPLATE_URL_SPEC_ANDROID_PACKAGE,
             TEMPLATE_URL_SPEC_CONFIG_APP_ID,
             TEMPLATE_URL_SPEC_IOS_APP_NAME,
             TEMPLATE_URL_SPEC_IOS_APP_STORE_ID,
             TEMPLATE_URL_SPEC_IOS_URL,
             TEMPLATE_URL_SPEC_IPHONE_APP_NAME,
             TEMPLATE_URL_SPEC_IPHONE_APP_STORE_ID,
             TEMPLATE_URL_SPEC_IPHONE_URL,
             TEMPLATE_URL_SPEC_IPAD_APP_NAME,
             TEMPLATE_URL_SPEC_IPAD_APP_STORE_ID,
             TEMPLATE_URL_SPEC_IPAD_URL,
             TEMPLATE_URL_SPEC_WEB_SHOULD_FALLBACK,
             TEMPLATE_URL_SPEC_WEB_URL,
             TEMPLATE_URL_SPEC_WINDOWS_PHONE_APP_ID,
             TEMPLATE_URL_SPEC_WINDOWS_PHONE_APP_NAME,
             TEMPLATE_URL_SPEC_WINDOWS_PHONE_URL,

             {% for column_name in object_story_list %}
             PARSE_JSON(OBJECT_STORY_SPEC):{{column_name}}::varchar as "OBJECT_STORY_SPEC_{{column_name}}"{%- if not loop.last %},{% endif -%}
             {% endfor %},

             {% for column_name in degrees_of_freedom_list %}
             PARSE_JSON(DEGREES_OF_FREEDOM_SPEC):{{column_name}}::varchar as "DEGREES_OF_FREEDOM_SPEC_{{column_name}}"{%- if not loop.last %},{% endif -%}
             {% endfor %},

             {% for column_name in asset_feed_list %}
             PARSE_JSON(ASSET_FEED_SPEC):{{column_name}}::varchar as "ASSET_FEED_SPEC_{{column_name}}"{%- if not loop.last %},{% endif -%}
             {% endfor %},
             _SDC_BATCHED_AT,

             STRTOK_TO_ARRAY(URL_TAGS, '&') as TAGS,
             OBJECT_CONSTRUCT('key', STRTOK_TO_ARRAY(TAGS[0], '=')[0], 'type', 'AD', 'value', STRTOK_TO_ARRAY(TAGS[0], '=')[1]) as UTM_SOURCE,
             OBJECT_CONSTRUCT('key', STRTOK_TO_ARRAY(TAGS[1], '=')[0], 'type', 'AD', 'value', STRTOK_TO_ARRAY(TAGS[1], '=')[1]) as UTM_MEDIUM,
             OBJECT_CONSTRUCT('key', STRTOK_TO_ARRAY(TAGS[2], '=')[0], 'type', 'AD', 'value', STRTOK_TO_ARRAY(TAGS[2], '=')[1]) as UTM_CAMPAIGN,
             OBJECT_CONSTRUCT('key', STRTOK_TO_ARRAY(TAGS[3], '=')[0], 'type', 'AD', 'value', STRTOK_TO_ARRAY(TAGS[3], '=')[1]) as UTM_CONTENT,
             OBJECT_CONSTRUCT('key', STRTOK_TO_ARRAY(TAGS[4], '=')[0], 'type', 'AD', 'value', STRTOK_TO_ARRAY(TAGS[4], '=')[1]) as UTM_TERM,
             OBJECT_CONSTRUCT('key', STRTOK_TO_ARRAY(TAGS[5], '=')[0], 'type', 'AD', 'value', STRTOK_TO_ARRAY(TAGS[5], '=')[1]) as TW_ADID,
             ARRAY_CONSTRUCT(UTM_TERM, TW_ADID, UTM_CAMPAIGN, UTM_MEDIUM, UTM_SOURCE, UTM_CONTENT) as URL_TAGS_ARRAY

      FROM {{ source('tap_facebook', 'creatives') }} as creative_history)
    