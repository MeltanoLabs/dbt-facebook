{{
   config(
     materialized='view'
   )
}}

{% set json_column_query %}
select distinct json.key as column_name

FROM (SELECT ARRAY_TO_STRING(PARSE_JSON(RECOMMENDATIONS), '') as RECOMMENDATIONS_COLUMNS FROM {{ source('tap_facebook', 'ads') }}),

lateral flatten(input=>PARSE_JSON(RECOMMENDATIONS_COLUMNS)) json
{% endset %}
 
{% set recommendations_results = run_query(json_column_query) %}

{% if execute %}
{# Return the first column #}
{% set recommendations_list = recommendations_results.columns[0].values() %}
{% else %}
{% set recommendations_list = [] %}
{% endif %}


SELECT AD_ID,
       AD_UPDATED_TIME,
       CODE::number as CODE,
       BLAME_FIELD,
       CONFIDENCE,
       IMPORTANCE,
       MESSAGE,
       TITLE,
       RECOMMENDATION_DATA

FROM (SELECT ID as AD_ID,
             UPDATED_TIME as AD_UPDATED_TIME,

             {% for column_name in recommendations_list %}
             PARSE_JSON(RECOMMENDATIONS_COLUMNS):{{column_name}}::varchar as {{column_name}}{%- if not loop.last %},{% endif -%}
             {% endfor %},

             RECOMMENDATION_DATA

      FROM (SELECT ACCOUNT_ID,
                   TO_TIMESTAMP_NTZ(CREATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as CREATED_TIME,
                   ID,
                   ARRAY_TO_STRING(PARSE_JSON(RECOMMENDATIONS), '') as RECOMMENDATIONS_COLUMNS,
                   TO_TIMESTAMP_NTZ(UPDATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as UPDATED_TIME,
                   RECOMMENDATION_DATA
            FROM {{ source('tap_facebook', 'ads') }} as meltano_ad_recommendation))
