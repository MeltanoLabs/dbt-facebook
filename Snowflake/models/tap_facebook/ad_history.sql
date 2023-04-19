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
       CONVERSION_DOMAIN,
       CONVERSION_SPECS,
       TO_TIMESTAMP_NTZ(CREATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as CREATED_TIME,
       CREATIVE,
       EFFECTIVE_STATUS,
       ID,
       LAST_UPDATED_BY_APP_ID,
       NAME,
       RECOMMENDATIONS,
       SOURCE_AD_ID as AD_SOURCE_ID,
       STATUS,
       TRACKING_SPECS,
       TO_TIMESTAMP_NTZ(UPDATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as UPDATED_TIME,
       GLOBAL_ADS_ABOUT_SOCIAL_ISSUES_ELECTIONS_OR_POLITICS,
       GLOBAL_ADULT_CONTENT,
       GLOBAL_ADVERTISING_POLICIES,
       GLOBAL_BRAND_USAGE_IN_ADS,
       GLOBAL_CIRCUMVENTING_SYSTEMS,
       GLOBAL_COMMERCIAL_EXPLOITATION_OF_CRISES_AND_CONTROVERSIAL_EVENTS,
       GLOBAL_DISCRIMINATORY_PRACTICES,
       GLOBAL_ILLEGAL_PRODUCTS_OR_SERVICES,
       GLOBAL_NONEXISTENT_FUNCTIONALITY,
       GLOBAL_NON_FUNCTIONAL_LANDING_PAGE,
       GLOBAL_PERSONAL_ATTRIBUTES,
       GLOBAL_PERSONAL_HEALTH_AND_APPEARANCE,
       GLOBAL_SENSATIONAL_CONTENT,
       GLOBAL_SPYWARE_OR_MALWARE,
       GLOBAL_UNREALISTIC_OUTCOMES,
       GLOBAL_UNSAFE_SUBSTANCES,
       PLACEMENT_SPECIFIC_FACEBOOK_ADS_ABOUT_SOCIAL_ISSUES_ELECTIONS_OR_POLITICS,
       PLACEMENT_SPECIFIC_FACEBOOK_ADULT_CONTENT,
       PLACEMENT_SPECIFIC_FACEBOOK_ADVERTISING_POLICIES,
       PLACEMENT_SPECIFIC_FACEBOOK_BRAND_USAGE_IN_ADS,
       PLACEMENT_SPECIFIC_FACEBOOK_CIRCUMVENTING_SYSTEMS,
       PLACEMENT_SPECIFIC_FACEBOOK_COMMERCIAL_EXPLOITATION_OF_CRISES_AND_CONTROVERSIAL_EVENTS,
       PLACEMENT_SPECIFIC_FACEBOOK_DISCRIMINATORY_PRACTICES,
       PLACEMENT_SPECIFIC_FACEBOOK_ILLEGAL_PRODUCTS_OR_SERVICES,
       PLACEMENT_SPECIFIC_FACEBOOK_NONEXISTENT_FUNCTIONALITY,
       PLACEMENT_SPECIFIC_FACEBOOK_NON_FUNCTIONAL_LANDING_PAGE,
       PLACEMENT_SPECIFIC_FACEBOOK_PERSONAL_ATTRIBUTES,
       PLACEMENT_SPECIFIC_FACEBOOK_PERSONAL_HEALTH_AND_APPEARANCE,
       PLACEMENT_SPECIFIC_FACEBOOK_SENSATIONAL_CONTENT,
       PLACEMENT_SPECIFIC_FACEBOOK_SPYWARE_OR_MALWARE,
       PLACEMENT_SPECIFIC_FACEBOOK_UNREALISTIC_OUTCOMES,
       PLACEMENT_SPECIFIC_FACEBOOK_UNSAFE_SUBSTANCES,
       PLACEMENT_SPECIFIC_INSTAGRAM_ADS_ABOUT_SOCIAL_ISSUES_ELECTIONS_OR_POLITICS,
       PLACEMENT_SPECIFIC_INSTAGRAM_ADULT_CONTENT,
       PLACEMENT_SPECIFIC_INSTAGRAM_ADVERTISING_POLICIES,
       PLACEMENT_SPECIFIC_INSTAGRAM_BRAND_USAGE_IN_ADS,
       PLACEMENT_SPECIFIC_INSTAGRAM_CIRCUMVENTING_SYSTEMS,
       PLACEMENT_SPECIFIC_INSTAGRAM_COMMERCIAL_EXPLOITATION_OF_CRISES_AND_CONTROVERSIAL_EVENTS,
       PLACEMENT_SPECIFIC_INSTAGRAM_ILLEGAL_PRODUCTS_OR_SERVICES,
       PLACEMENT_SPECIFIC_INSTAGRAM_NONEXISTENT_FUNCTIONALITY,
       PLACEMENT_SPECIFIC_INSTAGRAM_NON_FUNCTIONAL_LANDING_PAGE,
       PLACEMENT_SPECIFIC_INSTAGRAM_PERSONAL_ATTRIBUTES,
       PLACEMENT_SPECIFIC_INSTAGRAM_PERSONAL_HEALTH_AND_APPEARANCE,
       PLACEMENT_SPECIFIC_INSTAGRAM_SENSATIONAL_CONTENT,
       PLACEMENT_SPECIFIC_INSTAGRAM_SPYWARE_OR_MALWARE,
       PLACEMENT_SPECIFIC_INSTAGRAM_UNREALISTIC_OUTCOMES,
       PLACEMENT_SPECIFIC_INSTAGRAM_UNSAFE_SUBSTANCES, 



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
