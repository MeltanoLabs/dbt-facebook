{{
   config(
     materialized='table'
   )
}}

{% set json_column_query %}
select distinct json.key as column_name

FROM {{ source('tap_facebook', 'ads')}},

lateral flatten(input=>CONVERSION_SPECS) json

{% endset %}

{% set json_column_query %}
select distinct json.key as column_name

FROM {{ source('tap_facebook', 'ads')}},

lateral flatten(input=>TRACKING_SPECS) json

{% endset %}
 
{% set results = run_query(json_column_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}
 
SELECT ACCOUNT_ID,
       ADSET_ID,
       APPLICATION,
       CAMPAIGN_ID,
       CONVERSION_SPECS,
       TO_TIMESTAMP_NTZ(CREATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as CREATED_TIME,
       CREATIVE,
       DATASET,
       EVENT,
       EVENT_CREATOR,
       EVENT_TYPE,
       FB_PIXEL,
       FB_PIXEL_EVENT,
       ID,
       0 as INDEX,
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
       TO_TIMESTAMP_NTZ(UPDATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as UPDATED_TIME


{% for column_name in results_list %}
CONVERSION_SPECS:{{column_name}}::varchar as {{column_name}}{%- if not loop.last %},{% endif -%}
{% endfor %}

{% for column_name in results_list %}
TRACKING_SPECS:{{column_name}}::varchar as {{column_name}}{%- if not loop.last %},{% endif -%}
{% endfor %}

       /*TODO: Add columns 'Action_type' and 'Conversion_id'*/

FROM {{ source('tap_facebook', 'ads') }} as meltano_ad_conversion
