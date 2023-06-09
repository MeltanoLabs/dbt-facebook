SELECT
    id,
    TO_TIMESTAMP_NTZ(
        updated_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM'
    ) AS updated_time,
    account_id,
    source_campaign_id,
    ad_strategy_group_id,
    ad_strategy_id,
    bid_strategy,
    boosted_object_id,
    budget_rebalance_flag,
    CAST(budget_remaining AS float) AS budget_remaining,
    buying_type,
    can_create_brand_lift_study,
    can_use_spend_cap,
    configured_status,
    TO_TIMESTAMP_NTZ(
        created_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM'
    ) AS created_time,
    daily_budget,
    effective_status,
    is_skadnetwork_attribution,
    TO_TIMESTAMP_NTZ(
        last_budget_toggling_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM'
    ) AS last_budget_toggling_time,
    lifetime_budget,
    name,
    objective,
    smart_promotion_type,
    special_ad_category,
    spend_cap,
    TO_TIMESTAMP_NTZ(start_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') AS start_time,
    status,
    TO_TIMESTAMP_NTZ(stop_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') AS stop_time,
    topline_id,
    pacing_type,
    special_ad_categories,
    special_ad_category_country,
    promoted_object_application_id,
    promoted_object_custom_conversion_id,
    promoted_object_custom_event_str,
    promoted_object_custom_event_type,
    promoted_object_event_id,
    promoted_object_object_store_url,
    promoted_object_offer_id,
    promoted_object_offline_conversion_data_set_id,
    promoted_object_page_id,
    promoted_object_pixel_aggregation_rule,
    promoted_object_pixel_id,
    promoted_object_pixel_rule,
    promoted_object_place_page_set_id,
    promoted_object_product_catalog_id,
    promoted_object_product_set_id,
    promoted_object_retention_days,
    _sdc_batched_at


FROM {{ source('tap_facebook', 'campaigns') }}
