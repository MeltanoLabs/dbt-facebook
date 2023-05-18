SELECT
    ad_id,
    CAST(date_start AS date) AS date,
    account_id,
    impressions,
    inline_link_clicks,
    reach,
    CAST(cost_per_inline_link_click AS float) AS cost_per_inline_link_click,
    CAST(cpc AS float) AS cpc,
    CAST(cpm AS float) AS cpm,
    CAST(ctr AS float) AS ctr,
    CAST(frequency AS float) AS frequency,
    CAST(spend AS float) AS spend,
    ad_name,
    adset_name,
    inline_link_click_ctr,
    _sdc_batched_at


FROM {{ source('tap_facebook', 'adsinsights') }}
