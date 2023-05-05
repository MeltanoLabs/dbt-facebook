{{
   config(
     materialized='view'
   )
}}

{% set json_column_query %}
select distinct json.key as column_name

FROM {{ source('tap_facebook', 'customaudiences') }},

lateral flatten(input=>PARSE_JSON(DATA_SOURCE)) json
{% endset %}
 
{% set datasource_results = run_query(json_column_query) %}

{% if execute %}
{# Return the first column #}
{% set datasource_list = datasource_results.columns[0].values() %}
{% else %}
{% set datasource_list = [] %}
{% endif %}

{% set json_column_query %}
select distinct json.key as column_name

FROM {{ source('tap_facebook', 'customaudiences') }},

lateral flatten(input=>PARSE_JSON(DELIVERY_STATUS)) json
{% endset %}
 
{% set deliverystatus_results = run_query(json_column_query) %}

{% if execute %}
{# Return the first column #}
{% set deliverystatus_list = deliverystatus_results.columns[0].values() %}
{% else %}
{% set deliverystatus_list = [] %}
{% endif %}

{% set json_column_query %}
select distinct json.key as column_name

FROM {{ source('tap_facebook', 'customaudiences') }},

lateral flatten(input=>PARSE_JSON(OPERATION_STATUS)) json
{% endset %}
 
{% set operationstatus_results = run_query(json_column_query) %}

{% if execute %}
{# Return the first column #}
{% set operationstatus_list = operationstatus_results.columns[0].values() %}
{% else %}
{% set operationstatus_list = [] %}
{% endif %}

{% set json_column_query %}
select distinct json.key as column_name

FROM {{ source('tap_facebook', 'customaudiences') }},

lateral flatten(input=>PARSE_JSON(PERMISSION_FOR_ACTIONS)) json
{% endset %}
 
{% set permission_results = run_query(json_column_query) %}

{% if execute %}
{# Return the first column #}
{% set permission_list = permission_results.columns[0].values() %}
{% else %}
{% set permission_list = [] %}
{% endif %}
 
SELECT ID,
       UPDATED_TIME,
       ACCOUNT_ID,
       APPROXIMATE_COUNT_LOWER_BOUND,
       APPROXIMATE_COUNT_UPPER_BOUND,
       CUSTOMER_FILE_SOURCE,
       DESCRIPTION,
       IS_VALUE_BASED,
       NAME,
       OPT_OUT_LINK,
       PIXEL_ID,
       RETENTION_DAYS,
       RULE,
       RULE_AGGREGATION,
       SUBTYPE,
       CREATION_PARAMS as DATA_SOURCE_CREATION_PARAMS,
       SUB_TYPE as DATA_SOURCE_SUB_TYPE,
       TYPE as DATA_SOURCE_TYPE,
       "DELIVERY_STATUS_code"::number as DELIVERY_STATUS_CODE,
       "DELIVERY_STATUS_description" as DELIVERY_STATUS_DESCRIPTION,
       EXTERNAL_EVENT_SOURCE_ID,
       EXTERNAL_EVENT_SOURCE_AUTOMATIC_MATCHING_FIELDS,
       EXTERNAL_EVENT_SOURCE_CAN_PROXY,
       EXTERNAL_EVENT_SOURCE_CODE,
       EXTERNAL_EVENT_SOURCE_CREATION_TIME,
       EXTERNAL_EVENT_SOURCE_DATA_USE_SETTING,
       EXTERNAL_EVENT_SOURCE_ENABLE_AUTOMATIC_MATCHING,
       EXTERNAL_EVENT_SOURCE_FIRST_PARTY_COOKIE_STATUS,
       EXTERNAL_EVENT_SOURCE_IS_CREATED_BY_BUSINESS,
       EXTERNAL_EVENT_SOURCE_IS_CRM,
       EXTERNAL_EVENT_SOURCE_IS_UNAVAILABLE,
       EXTERNAL_EVENT_SOURCE_LAST_FIRED_TIME,
       EXTERNAL_EVENT_SOURCE_NAME,
       LOOKALIKE_COUNTRY,
       LOOKALIKE_IS_FINANCIAL_SERVICE,
       LOOKALIKE_ORIGIN_EVENT_NAME,
       LOOKALIKE_ORIGIN_EVENT_SOURCE_NAME,
       LOOKALIKE_PRODUCT_SET_NAME,
       LOOKALIKE_RATIO,
       LOOKALIKE_STARTING_RATIO,
       LOOKALIKE_TYPE,
       "OPERATION_STATUS_code"::number as OPERATION_STATUS_CODE,
       "OPERATION_STATUS_description" as OPERATION_STATUS_DESCRIPTION,
       CAST("PERMISSION_FOR_ACTIONS_can_edit" as boolean) as PERMISSION_FOR_ACTIONS_CAN_EDIT,
       CAST("PERMISSION_FOR_ACTIONS_can_see_insight" as boolean) as PERMISSION_FOR_ACTIONS_CAN_SEE_INSIGHT,
       CAST("PERMISSION_FOR_ACTIONS_can_share" as boolean) as PERMISSION_FOR_ACTIONS_CAN_SHARE,
       CAST("PERMISSION_FOR_ACTIONS_subtype_supports_lookalike" as boolean) as PERMISSION_FOR_ACTIONS_SUBTYPE_SUPPORTS_LOOKALIKE,
       CAST("PERMISSION_FOR_ACTIONS_supports_recipient_lookalike" as boolean) as PERMISSION_FOR_ACTIONS_SUPPORTS_RECIPIENT_LOOKALIKE,
       CONTENT_UPDATED_TIME,
       CREATED_TIME
FROM (SELECT ID,
             CAST(TIME_UPDATED as datetime) as UPDATED_TIME,
             ACCOUNT_ID,
             APPROXIMATE_COUNT_LOWER_BOUND,
             APPROXIMATE_COUNT_UPPER_BOUND,
             CUSTOMER_FILE_SOURCE,
             DESCRIPTION,
             IS_VALUE_BASED,
             NAME,
             OPT_OUT_LINK,
             PIXEL_ID,
             RETENTION_DAYS,
             RULE,
             RULE_AGGREGATION,
             SUBTYPE,

             {% for column_name in datasource_list %}
             PARSE_JSON(DATA_SOURCE):{{column_name}}::varchar as {{column_name}}{%- if not loop.last %},{% endif -%}
             {% endfor %},

             {% for column_name in deliverystatus_list %}
             PARSE_JSON(DELIVERY_STATUS):{{column_name}}::varchar as "DELIVERY_STATUS_{{column_name}}"{%- if not loop.last %},{% endif -%}
             {% endfor %},

             EXTERNAL_EVENT_SOURCE_ID,
             EXTERNAL_EVENT_SOURCE_AUTOMATIC_MATCHING_FIELDS,
             EXTERNAL_EVENT_SOURCE_CAN_PROXY,
             EXTERNAL_EVENT_SOURCE_CODE,
             EXTERNAL_EVENT_SOURCE_CREATION_TIME,
             EXTERNAL_EVENT_SOURCE_DATA_USE_SETTING,
             EXTERNAL_EVENT_SOURCE_ENABLE_AUTOMATIC_MATCHING,
             EXTERNAL_EVENT_SOURCE_FIRST_PARTY_COOKIE_STATUS,
             EXTERNAL_EVENT_SOURCE_IS_CREATED_BY_BUSINESS,
             EXTERNAL_EVENT_SOURCE_IS_CRM,
             EXTERNAL_EVENT_SOURCE_IS_UNAVAILABLE,
             EXTERNAL_EVENT_SOURCE_LAST_FIRED_TIME,
             EXTERNAL_EVENT_SOURCE_NAME,
             LOOKALIKE_COUNTRY,
             LOOKALIKE_IS_FINANCIAL_SERVICE,
             LOOKALIKE_ORIGIN_EVENT_NAME,
             LOOKALIKE_ORIGIN_EVENT_SOURCE_NAME,
             LOOKALIKE_PRODUCT_SET_NAME,
             CAST(LOOKALIKE_RATIO as float) as LOOKALIKE_RATIO,
             CAST(LOOKALIKE_STARTING_RATIO as float) as LOOKALIKE_STARTING_RATIO,
             LOOKALIKE_TYPE,

             {% for column_name in operationstatus_list %}
             PARSE_JSON(OPERATION_STATUS):{{column_name}}::varchar as "OPERATION_STATUS_{{column_name}}"{%- if not loop.last %},{% endif -%}
             {% endfor %},

             {% for column_name in permission_list %}
             PARSE_JSON(PERMISSION_FOR_ACTIONS):{{column_name}}::varchar as "PERMISSION_FOR_ACTIONS_{{column_name}}"{%- if not loop.last %},{% endif -%}
             {% endfor %},

             CAST(TIME_CONTENT_UPDATED as datetime) as CONTENT_UPDATED_TIME,
             CAST(TIME_CREATED as datetime) as CREATED_TIME

FROM {{ source('tap_facebook', 'customaudiences') }} as meltano_custom_audiences)
 