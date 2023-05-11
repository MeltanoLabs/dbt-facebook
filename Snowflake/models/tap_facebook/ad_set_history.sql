SELECT id::number as id,
       TO_TIMESTAMP_NTZ(updated_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as updated_time,
       campaign_id::number as campaign_id,
       account_id::number as account_id,
       asset_feed_id::number as asset_feed_id,
       bid_amount::number as bid_amount,
       bid_strategy,
       billing_event,
       budget_remaining::number as budget_remaining,
       configured_status,
       TO_TIMESTAMP_NTZ(created_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as created_time,
       daily_budget::number as daily_budget,
       daily_min_spend_target::number as daily_min_spend_target,
       daily_spend_cap::number as daily_spend_cap,
       destination_type,
       effective_status,
       TO_TIMESTAMP_NTZ(end_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as end_time,
       instagram_actor_id::number as instagram_actor_id,
       is_dynamic_creative::boolean as is_dynamic_creative,
       lifetime_budget::number as lifetime_budget,
       lifetime_imps::number as lifetime_imps,
       lifetime_min_spend_target::number as lifetime_min_spend_target,
       lifetime_spend_cap::number as lifetime_spend_cap,
       multi_optimization_goal_weight,
       name,
       optimization_goal,
       optimization_sub_event,
       recurring_budget_semantics::boolean as recurring_budget_semantics,
       review_feedback,
       rf_prediction_id,
       status,
       TO_TIMESTAMP_NTZ(start_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as start_time,
       use_new_app_click::boolean as use_new_app_click,
       promoted_object_application_id,
       promoted_object_custom_conversion_id::number as promoted_object_custom_conversion_id,
       promoted_object_custom_event_str,
       promoted_object:custom_event_type::varchar as promoted_object_custom_event_type,
       promoted_object_event_id::number as promoted_object_event_id,
       promoted_object_object_store_url,
       promoted_object_offer_id::number as promoted_object_offer_id,
       promoted_object_offline_conversion_data_set_id::number as promoted_object_offline_conversion_data_set_id,
       promoted_object_page_id::number as promoted_object_page_id,
       promoted_object_pixel_aggregation_rule,
       promoted_object:pixel_id::number as promoted_object_pixel_id,
       promoted_object_pixel_rule,
       promoted_object_place_page_set_id::number as promoted_object_place_page_set_id,
       promoted_object_product_catalog_id::number as promoted_object_product_catalog_id,
       promoted_object_product_set_id::number as promoted_object_product_set_id,
       promoted_object_retention_days,
       learning_stage_info:attribution_windows as learning_stage_info_attribution_windows,
       learning_stage_info:conversions::number as learning_stage_info_conversions,
       learning_stage_info:last_sig_edit_ts::number as learning_stage_info_last_sig_edit_ts,
       learning_stage_info:status::varchar as learning_stage_info_status,
       targeting_app_install_state,
       targeting:age_max::number as targeting_age_max,
       targeting:age_min::number as targeting_age_min,
       targeting:genders as targeting_genders,
       targeting_behaviors,
       targeting_interests,
       targeting_relationship_statuses,
       targeting_life_events,
       targeting_industries,
       targeting_income,
       targeting_family_statuses,
       targeting_audience_network_positions,
       targeting_connections,
       targeting:device_platforms as targeting_device_platforms,
       targeting_effective_audience_network_positions,
       targeting_excluded_connections,
       targeting_excluded_publisher_categories,
       targeting_excluded_publisher_list_ids,
       targeting_excluded_user_device,
       targeting:facebook_positions as targeting_facebook_positions,
       targeting_friends_of_connections,
       targeting:instagram_positions as targeting_instagram_positions,
       targeting:publisher_platforms as targeting_publisher_platforms,
       targeting_user_os,
       targeting_user_device,
       targeting_wireless_carrier,
       targeting_education_schools,
       targeting_education_statuses,
       targeting_college_years,
       targeting_education_majors,
       targeting_work_employers,
       targeting_work_positions,
       targeting_locales,
       targeting_user_adclusters,
       targeting_flexible_spec,
       targeting_exclusions,
       targeting:geo_locations.countries as targeting_geo_locations_countries,
       targeting_geo_locations_regions,
       targeting_geo_locations_cities,
       targeting_geo_locations_zips,
       targeting_geo_locations_places,
       targeting_geo_locations_custom_locations,
       targeting_geo_locations_geo_markets,
       targeting_geo_locations_electoral_district,
       targeting:geo_locations.location_types as targeting_geo_locations_location_types,
       targeting_geo_locations_country_groups,
       targeting_excluded_geo_locations_countries,
       targeting_excluded_geo_locations_regions,
       targeting_excluded_geo_locations_cities,
       targeting_excluded_geo_locations_zips,
       targeting_excluded_geo_locations_places,
       targeting_excluded_geo_locations_custom_locations,
       targeting_excluded_geo_locations_geo_markets,
       targeting_excluded_geo_locations_electoral_district,
       targeting_excluded_geo_locations_location_types,
       targeting_excluded_geo_locations_country_groups,

       source_adset_id::number as adset_source_id,
       bid_amount::number as bid_info_actions,
       bid_info:IMPRESSIONS::number as bid_info_impressions,
       _sdc_batched_at

FROM {{ source('tap_facebook', 'adsets') }} as meltano_ad_set_history