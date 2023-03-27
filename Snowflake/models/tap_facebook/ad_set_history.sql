{{
   config(
     materialized='table'
   )
}}

{% set json_column_query %}
select distinct json.key as column_name

FROM {{ source('tap_facebook', 'ad_set_history')}},

lateral flatten(input=>PROMOTED_OBJECT) json
{% endset %}
 
{% set results = run_query(json_column_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}


select ACCOUNT_ID,
       ADLABELS,
       ASSET_FEED_ID,
       ATTRIBUTION_SPEC,
       BID_AMOUNT,
       BID_INFO,
       BID_INFO_ACTIONS,
       BID_INFO_IMPRESSIONS,
       BID_STRATEGY,
       BILLING_EVENT,
       BUDGET_REMAINING,
       CAMPAIGN_ATTRIBUTION,
       CAMPAIGN_ID,
       CONFIGURED_STATUS,
       CREATED_TIME,
       DAILY_BUDGET,
       DAILY_MIN_SPEND_TARGET,
       DAILY_SPEND_CAP,
       DESTINATION_TYPE,
       EFFECTIVE_STATUS,
       END_TIME,
       ID,
       INSTAGRAM_ACTOR_ID,
       IS_DYNAMIC_CREATIVE,
       LEARNING_STAGE_INFO,
       LIFETIME_BUDGET,
       LIFETIME_IMPS,
       LIFETIME_MIN_SPEND_TARGET,
       LIFETIME_SPEND_CAP,
       MULTI_OPTIMIZATION_GOAL_WEIGHT,
       NAME,
       OPTIMIZATION_GOAL,
       OPTIMIZATION_SUB_EVENT,
       PACING_TYPE,
       PROMOTED_OBJECT,
       PROMOTED_OBJECT_APPLICATION_TYPE,
       PROMOTED_OBJECT_CUSTOM_CONVERSION_ID,
       PROMOTED_OBJECT_CUSTOM_EVENT_STR,
       PROMOTED_OBJECT_CUSTOM_EVENT_TYPE,
       PROMOTED_OBJECT_EVENT_ID,
       PROMOTED_OBJECT_OBJECT_STORE_URL,
       PROMOTED_OBJECT_OFFER_ID,
       PROMOTED_OBJECT_OFFLINE_CONVERSION_DATA_SET_ID,
       PROMOTED_OBJECT_PAGE_ID,
       PROMOTED_OBJECT_PIXEL_AGGREGATION_RULE,
       PROMOTED_OBJECT_PIXEL_ID,
       PROMOTED_OBJECT_PIXEL_RULE,
       PROMOTED_OBJECT_PLACE_PAGE_SET_ID,
       PROMOTED_OBJECT_PRODUCT_CATALOG_ID,
       PROMOTED_OBJECT_PRODUCT_SET_ID,
       PROMOTED_OBJECT_RETENTION_DAYS,
       RECURRING_BUDGET_SEMANTICS,
       REVIEW_FEEDBACK,
       RF_PREDICTION_ID,
       SOURCE_ADSET_ID,
       START_TIME,
       STATUS,
       TARGETING_OPTIMIZATION_TYPES,
       UPDATED_TIME,
       USE_NEW_APP_CLICK,

{% for column_name in results_list %}
PROMOTED_OBJECT:{{column_name}}::varchar as {{column_name}}{%- if not loop.last %},{% endif -%}
{% endfor %}


FROM {{ source('tap_facebook', 'ad_set_history') }} as meltano_ad_set_history
