{{
   config(
     materialized='view'
   )
}}

SELECT AD_ID,
       DATE,
       ACTION_TYPE,
       VALUE,
       _7_D_CLICK,
       _SDC_BATCHED_AT,
       INLINE::float as INLINE,
       _1_D_VIEW
       /* INDEX column*/

FROM (SELECT AD_ID,
             DATE, 
             REPLACE(COST_PER_ACTION:"action_type", '"','')::varchar as ACTION_TYPE,
             REPLACE(COST_PER_ACTION:"value", '"','')::float as VALUE,
             REPLACE(COST_PER_ACTION:"7d_click", '"','')::float as _7_D_CLICK,
             _SDC_BATCHED_AT,
             REPLACE(COST_PER_ACTION:"1d_view", '"', '')::float as _1_D_VIEW,
             CASE WHEN _7_D_CLICK IS NULL AND _1_D_VIEW IS NULL THEN VALUE ELSE NULL END as INLINE

      FROM (SELECT AD_ID,
                   COST_PER_ACTION_TYPE_COLUMN.value as COST_PER_ACTION,
                   DATE,
                   _SDC_BATCHED_AT
       
            FROM (SELECT AD_ID,
                         CAST(DATE_START as date) as DATE,
                         ACCOUNT_ID,
                         PARSE_JSON(COST_PER_ACTION_TYPE) as COST_PER_ACTION_COLUMN,
                         _SDC_BATCHED_AT

                  FROM {{ source('tap_facebook', 'adsinsights') }}) meltano_cost_per_action_type,
                  lateral flatten(input=>COST_PER_ACTION_COLUMN) COST_PER_ACTION_TYPE_COLUMN))

