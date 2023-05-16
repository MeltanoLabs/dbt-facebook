SELECT
    account_id::number AS account_id,
    id,
    actor_id::number AS actor_id,
    applink_treatment,
    authorization_category,
    REPLACE(
        PARSE_JSON(object_story_spec):video_data:title, '"'
    )::varchar AS body,
    branded_content_sponsor_page_id::number AS branded_content_sponsor_page_id,
    bundle_folder_id::number AS bundle_folder_id,
    call_to_action_type,
    categorization_criteria,
    category_media_source,
    destination_set_id::number AS destination_set_id,
    dynamic_ad_voice,
    effective_authorization_category,
    effective_instagram_media_id::number AS effective_instagram_media_id,
    effective_instagram_story_id::number AS effective_instagram_story_id,
    effective_object_story_id,
    enable_direct_install,
    image_hash,
    image_url,
    instagram_actor_id,
    instagram_permalink_url,
    instagram_story_id,
    link_destination_display_url,
    link_og_id,
    link_url,
    messenger_sponsored_message,
    name,
    object_id,
    object_store_url,
    object_story_id,
    object_type,
    object_url,
    place_page_set_id,
    playable_asset_id,
    product_set_id::number AS product_set_id,
    source_instagram_media_id,
    status,
    template_url,
    thumbnail_id,
    thumbnail_url,
    title,
    use_page_actor_override,
    video_id,
    carousel_ad_link,
    REPLACE(
        PARSE_JSON(
            ARRAY_TO_STRING(
                ARRAY_SLICE(
                    PARSE_JSON(object_story_spec):link_data:child_attachments,
                    0,
                    1
                ),
                ''
            )
        ):link,
        '"',
        ''
    ) AS page_link,
    template_page_link,

    ARRAY_CONSTRUCT(
        OBJECT_CONSTRUCT(
            'key',
            STRTOK_TO_ARRAY(STRTOK_TO_ARRAY(url_tags, '&')[4], '=')[0],
            'type',
            'AD',
            'value',
            STRTOK_TO_ARRAY(STRTOK_TO_ARRAY(url_tags, '&')[4], '=')[1]
        ),
        OBJECT_CONSTRUCT(
            'key',
            STRTOK_TO_ARRAY(STRTOK_TO_ARRAY(url_tags, '&')[5], '=')[0],
            'type',
            'AD',
            'value',
            STRTOK_TO_ARRAY(STRTOK_TO_ARRAY(url_tags, '&')[5], '=')[1]
        ),
        OBJECT_CONSTRUCT(
            'key',
            STRTOK_TO_ARRAY(STRTOK_TO_ARRAY(url_tags, '&')[2], '=')[0],
            'type',
            'AD',
            'value',
            STRTOK_TO_ARRAY(STRTOK_TO_ARRAY(url_tags, '&')[2], '=')[1]
        ),
        OBJECT_CONSTRUCT(
            'key',
            STRTOK_TO_ARRAY(STRTOK_TO_ARRAY(url_tags, '&')[1], '=')[0],
            'type',
            'AD',
            'value',
            STRTOK_TO_ARRAY(STRTOK_TO_ARRAY(url_tags, '&')[1], '=')[1]
        ),
        OBJECT_CONSTRUCT(
            'key',
            STRTOK_TO_ARRAY(STRTOK_TO_ARRAY(url_tags, '&')[0], '=')[0],
            'type',
            'AD',
            'value',
            STRTOK_TO_ARRAY(STRTOK_TO_ARRAY(url_tags, '&')[0], '=')[1]
        ),
        OBJECT_CONSTRUCT(
            'key',
            STRTOK_TO_ARRAY(STRTOK_TO_ARRAY(url_tags, '&')[3], '=')[0],
            'type',
            'AD',
            'value',
            STRTOK_TO_ARRAY(STRTOK_TO_ARRAY(url_tags, '&')[3], '=')[1]
        )

    ) AS url_tags,

    object_story_link_data_caption,
    REPLACE(
        PARSE_JSON(object_story_spec):link_data:message, '"', ''
    )::varchar AS object_story_link_data_message,
    object_story_link_data_description,
    REPLACE(
        PARSE_JSON(
            ARRAY_TO_STRING(
                ARRAY_SLICE(
                    PARSE_JSON(object_story_spec):link_data:child_attachments,
                    0,
                    1
                ),
                ''
            )
        ):link,
        '"',
        ''
    ) AS object_story_link_data_link,
    PARSE_JSON(
        object_story_spec
    ):link_data:child_attachments AS object_story_link_data_child_attachments,
    object_story_link_data_description AS object_story_link_data_app_link_spec_android,
    object_story_link_data_description AS object_story_link_data_app_link_spec_ios,
    object_story_link_data_description AS object_story_link_data_app_link_spec_ipad,
    object_story_link_data_description AS object_story_link_data_app_link_spec_iphone,
    template_app_link_spec_android,
    template_app_link_spec_ios,
    template_app_link_spec_ipad,
    template_app_link_spec_iphone,
    template_caption,
    template_child_attachments,
    template_description,
    template_link,
    template_message,
    page_message,
    REPLACE(
        PARSE_JSON(object_story_spec):link_data:call_to_action:value:link,
        '"',
        ''
    )::varchar AS video_call_to_action_value_link,
    platform_customizations_instagram_caption_ids,
    platform_customizations_instagram_image_hash,
    platform_customizations_instagram_image_url,
    platform_customizations_instagram_video_id,
    template_url_spec_android_app_name,
    template_url_spec_android_url,
    template_url_spec_android_package,
    template_url_spec_config_app_id,
    template_url_spec_ios_app_name,
    template_url_spec_ios_app_store_id,
    template_url_spec_ios_url,
    template_url_spec_iphone_app_name,
    template_url_spec_iphone_app_store_id,
    template_url_spec_iphone_url,
    template_url_spec_ipad_app_name,
    template_url_spec_ipad_app_store_id,
    template_url_spec_ipad_url,
    template_url_spec_web_should_fallback,
    template_url_spec_web_url,
    template_url_spec_windows_phone_app_id,
    template_url_spec_windows_phone_app_name,
    template_url_spec_windows_phone_url,
    PARSE_JSON(asset_feed_spec):link_urls AS asset_feed_spec_link_urls,
    _sdc_batched_at

FROM {{ source('tap_facebook', 'creatives') }}
