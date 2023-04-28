{{
   config(
     materialized='view'
   )
}}
 
SELECT ADSET_ID,
       CAST(DATE_START as date) as DATE,
       ACCOUNT_ID,
       IMPRESSIONS,
       INLINE_LINK_CLICKS,
       REACH,
       CAST(COST_PER_INLINE_LINK_CLICK as float) as COST_PER_INLINE_LINK_CLICK,
       CAST(CPC as float) as CPC,
       CAST(CPM as float) as CPM,
       CAST(CTR as float) as CTR,
       CAST(FREQUENCY as float) as FREQUENCY,
       CAST(SPEND as float) as SPEND,
       ADSET_NAME,
       CAMPAIGN_NAME,
       INLINE_LINK_CLICK_CTR,
       _SDC_BATCHED_AT


FROM {{ source('tap_facebook', 'adsinsights') }} as meltano_basic_ad_set
