SELECT
    id,
    CAST(time_updated AS datetime) AS updated_time,
    account_id,
    approximate_count_lower_bound,
    approximate_count_upper_bound,
    customer_file_source,
    description,
    is_value_based,
    name,
    opt_out_link,
    pixel_id,
    retention_days,
    rule,
    rule_aggregation,
    subtype,
    PARSE_JSON(
        data_source
    ):creation_params::varchar AS data_source_creation_params,
    PARSE_JSON(data_source):sub_type::varchar AS data_source_sub_type,
    PARSE_JSON(data_source):type::varchar AS data_source_type,
    PARSE_JSON(delivery_status):code::number AS delivery_status_code,
    PARSE_JSON(
        delivery_status
    ):description::varchar AS delivery_status_description,
    external_event_source_id,
    external_event_source_automatic_matching_fields,
    external_event_source_can_proxy,
    external_event_source_code,
    external_event_source_creation_time,
    external_event_source_data_use_setting,
    external_event_source_enable_automatic_matching,
    external_event_source_first_party_cookie_status,
    external_event_source_is_created_by_business,
    external_event_source_is_crm,
    external_event_source_is_unavailable,
    external_event_source_last_fired_time,
    external_event_source_name,
    lookalike_country,
    lookalike_is_financial_service,
    lookalike_origin_event_name,
    lookalike_origin_event_source_name,
    lookalike_product_set_name,
    CAST(lookalike_ratio AS float) AS lookalike_ratio,
    CAST(lookalike_starting_ratio AS float) AS lookalike_starting_ratio,
    lookalike_type,
    PARSE_JSON(operation_status):code::number AS operation_status_code,
    PARSE_JSON(
        operation_status
    ):description::varchar AS operation_status_description,
    CAST(
        PARSE_JSON(permission_for_actions):can_edit AS boolean
    ) AS permission_for_actions_can_edit,
    CAST(
        PARSE_JSON(permission_for_actions):can_see_insight AS boolean
    ) AS permission_for_actions_can_see_insight,
    CAST(
        PARSE_JSON(permission_for_actions):can_share AS boolean
    ) AS permission_for_actions_can_share,
    CAST(
        PARSE_JSON(permission_for_actions):subtype_supports_lookalike AS boolean
    ) AS permission_for_actions_subtype_supports_lookalike,
    CAST(
        PARSE_JSON(
            permission_for_actions
        ):supports_recipient_lookalike AS boolean
    ) AS permission_for_actions_supports_recipient_lookalike,
    CAST(time_content_updated AS datetime) AS content_updated_time,
    CAST(time_created AS datetime) AS created_time,
    _sdc_batched_at

FROM {{ source('tap_facebook', 'customaudiences') }}
