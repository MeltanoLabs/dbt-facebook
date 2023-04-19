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
       ADSET_ID,
       CAMPAIGN_ID,
       TO_TIMESTAMP_NTZ(CREATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as CREATED_TIME,
       CREATIVE,
       ID,
       NAME,
       TRACKING_SPECS,
       TO_TIMESTAMP_NTZ(UPDATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as UPDATED_TIME,

/*TODO: Add columns 'Action_type',
'Conversion_id',
APPLICATION,
DATASET,
EVENT,
EVENT_CREATOR,
EVENT_TYPE,
FB_PIXEL,
FB_PIXEL_EVENT,
INDEX,
LEADGEN,
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
*/


{% for column_name in creative_list %}
CREATIVE:{{column_name}}::varchar as "CREATIVE_ID"{%- if not loop.last %},{% endif -%}
{% endfor %},

{% for column_name in tracking_sepcs_list %}
TRACKING_SPECS:{{column_name}}::varchar as "TRACKING_SPECS_{{column_name}}"{%- if not loop.last %},{% endif -%}
{% endfor %}

FROM {{ source('tap_facebook', 'ads') }} as ad_tracking

