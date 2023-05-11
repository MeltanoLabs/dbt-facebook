SELECT ad_id,
       CAST(date_start as date) as date,
       account_id,
       impressions,
       inline_link_clicks,
       reach,
       CAST(cost_per_inline_link_click as float) as cost_per_inline_link_click,
       CAST(cpc as float) as cpc,
       CAST(cpm as float) as cpm,
       CAST(ctr as float) as ctr,
       CAST(frequency as float) as frequency,
       CAST(spend as float) as spend,
       ad_name,
       adset_name,
       inline_link_click_ctr,
       _sdc_batched_at


FROM {{ source('tap_facebook', 'adsinsights') }} as meltano_basic_ads