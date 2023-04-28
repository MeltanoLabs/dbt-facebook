{{
   config(
     materialized='view'
   )
}}

{% set json_column_query %}
select distinct json.key as column_name

FROM {{ source('tap_facebook', 'adlabels')}},

lateral flatten(input=>ACCOUNT) json
{% endset %}
 
{% set account_results = run_query(json_column_query) %}

{% if execute %}
{# Return the first column #}
{% set account_list = account_results.columns[0].values() %}
{% else %}
{% set account_list = [] %}
{% endif %}
 
SELECT ID as ACCOUNT_ACT_ID,
       CREATED_TIME,
       ACCOUNT_ID,
       UPDATED_TIME,
       ADLABELS_ID as ID       
FROM (SELECT ID as ADLABELS_ID,
             TO_TIMESTAMP_NTZ(CREATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as CREATED_TIME,
              ACCOUNT,
              TO_TIMESTAMP_NTZ(UPDATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as UPDATED_TIME,
              _SDC_BATCHED_AT,


      {% for column_name in account_list %}
      ACCOUNT:{{column_name}}::varchar as {{column_name}}{%- if not loop.last %},{% endif -%}
      {% endfor %}       

       FROM {{ source('tap_facebook', 'adlabels') }} as label_history)
