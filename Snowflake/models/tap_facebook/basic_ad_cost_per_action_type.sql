SELECT AD_ID,
       CAST(DATE_START as date) as DATE,
       REPLACE(PARSE_JSON(value):"action_type", '"','')::varchar as ACTION_TYPE,
       REPLACE(PARSE_JSON(value):"value", '"','')::float as VALUE,
       REPLACE(PARSE_JSON(value):"7d_click", '"','')::float as _7_D_CLICK,
       INDEX-1 as INDEX,
       _SDC_BATCHED_AT,
       CASE WHEN _7_D_CLICK IS NULL AND REPLACE(PARSE_JSON(value):"1d_view", '"', '') IS NULL THEN REPLACE(PARSE_JSON(value):"value", '"','') ELSE NULL END::float as INLINE,
       REPLACE(PARSE_JSON(value):"1d_view", '"', '')::float as _1_D_VIEW

FROM {{ source('tap_facebook', 'adsinsights') }} meltano_cost_per_action_type,
lateral split_to_table(input=>ARRAY_TO_STRING(PARSE_JSON(COST_PER_ACTION_TYPE), '|'), '|') COST_PER_ACTION_TYPE_COLUMN