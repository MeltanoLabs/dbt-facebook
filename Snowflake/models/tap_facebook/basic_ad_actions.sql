SELECT
    ad_id,
    CAST(date_start AS date) AS date,
    CAST(
        REPLACE(PARSE_JSON(value):"action_type", '"', '') AS varchar
    ) AS action_type,
    CAST(ROUND(REPLACE(PARSE_JSON(value):"value", '"', '')) AS float) AS value,
    CAST(
        ROUND(REPLACE(PARSE_JSON(value):"7d_click", '"', '')) AS float
    ) AS _7_d_click,
    index - 1 AS index,
    _sdc_batched_at,
    CAST(CASE
        WHEN
            _7_d_click IS NOT NULL
            AND REPLACE(PARSE_JSON(value):"1d_view", '"', '') IS NOT NULL
            THEN ROUND(REPLACE(PARSE_JSON(value):"value", '"', ''))
    END AS float) AS inline,
    CAST(
        ROUND(REPLACE(PARSE_JSON(value):"1d_view", '"', '')) AS float
    ) AS _1_d_view

FROM {{ source('tap_facebook', 'adsinsights') }},
    LATERAL SPLIT_TO_TABLE(
        input => ARRAY_TO_STRING(PARSE_JSON(cost_per_action_type), '|'), '|'
    )
