{{
   config(
     materialized='table'
   )
}}

{% set json_column_query %}
select distinct json.key as column_name

FROM {{ source('tap_facebook', 'adsets')}},

lateral flatten(input=>ATTRIBUTION_SPEC[0]) json
{% endset %}
 
{% set attribution_spec_results = run_query(json_column_query) %}

{% if execute %}
{# Return the first column #}
{% set attribution_spec_list = attribution_spec_results.columns[0].values() %}
{% else %}
{% set attribution_spec_list = [] %}
{% endif %}
 
SELECT ACCOUNT_ID,
       TO_TIMESTAMP_NTZ(CREATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as AD_SET_CREATED_TIME,
       ID,
       ATTRIBUTION_SPEC,
       TO_TIMESTAMP_NTZ(UPDATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as AD_SET_UPDATED_TIME,


{% for column_name in attribution_spec_list %}
ATTRIBUTION_SPEC:{{column_name}}::varchar as {{column_name}}{%- if not loop.last %},{% endif -%}
{% endfor %}

FROM {{ source('tap_facebook', 'adsets') }} as ad_set_attribution
