{{
   config(
     materialized='table'
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

FROM {{ source('tap_facebook', 'ads')}},

lateral flatten(input=>BID_INFO) json
{% endset %}
 
{% set bid_info_results = run_query(json_column_query) %}

{% if execute %}
{# Return the first column #}
{% set bid_info_list = bid_info_results.columns[0].values() %}
{% else %}
{% set bid_info_list = [] %}
{% endif %}

{% set json_column_query %}
select distinct json.key as column_name

FROM {{ source('tap_facebook', 'ads')}},

lateral flatten(input=>TRACKING_SPECS[0]) json
{% endset %}
 
{% set tracking_specs_results = run_query(json_column_query) %}

{% if execute %}
{# Return the first column #}
{% set tracking_sepcs_list = tracking_specs_results.columns[0].values() %}
{% else %}
{% set tracking_sepcs_list = [] %}
{% endif %}
 
SELECT ACCOUNT_ID,
       ADLABELS,
       ADSET_ID,
       BID_AMOUNT,
       BID_INFO,
       BID_TYPE,
       CAMPAIGN_ID,
       CONFIGURED_STATUS,
       CONVERSION_DOMAIN,
       CONVERSION_SPECS,
       CREATED_TIME,
       CREATIVE,
       EFFECTIVE_STATUS,
       ID,
       LAST_UPDATED_BY_APP_ID,
       NAME,
       RECOMMENDATIONS,
       SOURCE_AD_ID,
       STATUS,
       TRACKING_SPECS,
       UPDATED_TIME,


{% for column_name in creative_list %}
CREATIVE:{{column_name}}::varchar as "CREATIVE_ID"{%- if not loop.last %},{% endif -%}
{% endfor %},

{% for column_name in bid_info_list %}
BID_INFO:{{column_name}}::varchar as "BID_INFO_{{column_name}}"{%- if not loop.last %},{% endif -%}
{% endfor %},

{% for column_name in tracking_sepcs_list %}
TRACKING_SPECS:{{column_name}}::varchar as "TRACKING_SPECS_{{column_name}}"{%- if not loop.last %},{% endif -%}
{% endfor %}


FROM {{ source('tap_facebook', 'ads') }} as ad_history
