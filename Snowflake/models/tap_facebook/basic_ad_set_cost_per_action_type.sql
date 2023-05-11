SELECT adset_id,
       CAST(date_start as date) as date,
       REPLACE(PARSE_JSON(value):"action_type", '"','')::varchar as action_type,
       REPLACE(PARSE_JSON(value):"value", '"','')::float as value,
       REPLACE(PARSE_JSON(value):"7d_click", '"','')::float as _7_d_click,
       index-1 as index,
       _sdc_batched_at,
       CASE WHEN _7_d_click IS NULL AND REPLACE(PARSE_JSON(value):"1d_view", '"', '') IS NULL THEN REPLACE(PARSE_JSON(value):"value", '"','') ELSE NULL END::float as inline,
       REPLACE(PARSE_JSON(value):"1d_view", '"', '')::float as _1_d_view

FROM {{ source('tap_facebook', 'adsinsights') }},
lateral split_to_table(input=>ARRAY_TO_STRING(PARSE_JSON(cost_per_action_type), '|'), '|') cost_per_action_type_column