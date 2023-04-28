{{
   config(
     materialized='view'
   )
}}
 
SELECT ID,
       TO_TIMESTAMP_NTZ(UPDATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as UPDATED_TIME,
       ACCOUNT_ID,
       SOURCE_CAMPAIGN_ID,
       AD_STRATEGY_GROUP_ID,
       AD_STRATEGY_ID,
       BID_STRATEGY,
       BOOSTED_OBJECT_ID,
       BUDGET_REBALANCE_FLAG,
       CAST(BUDGET_REMAINING as float) as BUDGET_REMAINING,
       BUYING_TYPE,
       CAN_CREATE_BRAND_LIFT_STUDY,
       CAN_USE_SPEND_CAP,
       CONFIGURED_STATUS,
       TO_TIMESTAMP_NTZ(CREATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as CREATED_TIME,
       DAILY_BUDGET,
       EFFECTIVE_STATUS,
       IS_SKADNETWORK_ATTRIBUTION,
       TO_TIMESTAMP_NTZ(LAST_BUDGET_TOGGLING_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as LAST_BUDGET_TOGGLING_TIME,
       LIFETIME_BUDGET,
       NAME,
       OBJECTIVE,
       SMART_PROMOTION_TYPE,
       SPECIAL_AD_CATEGORY,
       SPEND_CAP,
       TO_TIMESTAMP_NTZ(START_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as START_TIME,
       STATUS,
       TO_TIMESTAMP_NTZ(STOP_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as STOP_TIME,
       TOPLINE_ID,
       PACING_TYPE,
       SPECIAL_AD_CATEGORIES,
       SPECIAL_AD_CATEGORY_COUNTRY,
       PROMOTED_OBJECT_APPLICATION_ID,
       PROMOTED_OBJECT_CUSTOM_CONVERSION_ID,
       PROMOTED_OBJECT_CUSTOM_EVENT_STR,
       PROMOTED_OBJECT_CUSTOM_EVENT_TYPE,
       PROMOTED_OBJECT_EVENT_ID,
       PROMOTED_OBJECT_OBJECT_STORE_URL,
       PROMOTED_OBJECT_OFFER_ID,
       PROMOTED_OBJECT_OFFLINE_CONVERSION_DATA_SET_ID,
       PROMOTED_OBJECT_PAGE_ID,
       PROMOTED_OBJECT_PIXEL_AGGREGATION_RULE,
       PROMOTED_OBJECT_PIXEL_ID,
       PROMOTED_OBJECT_PIXEL_RULE,
       PROMOTED_OBJECT_PLACE_PAGE_SET_ID,
       PROMOTED_OBJECT_PRODUCT_CATALOG_ID,
       PROMOTED_OBJECT_PRODUCT_SET_ID,
       PROMOTED_OBJECT_RETENTION_DAYS,
       _SDC_BATCHED_AT


FROM {{ source('tap_facebook', 'campaigns') }} as meltano_campaigns_history
