SELECT
    id AS ad_id,
    TO_TIMESTAMP_NTZ(
        updated_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM'
    ) AS ad_updated_time,
    index - 1 AS index,
    application,
    PARSE_JSON(creative):id::number AS creative,
    dataset,
    (PARSE_JSON(value):"event")::variant AS event,
    (PARSE_JSON(value):"event_type")::variant AS event_type,
    (PARSE_JSON(value):"fb_pixel")::variant AS fb_pixel,
    (PARSE_JSON(value):"fb_pixel_event")::variant AS fb_pixel_event,
    (PARSE_JSON(value):"leadgen")::variant AS leadgen,
    (PARSE_JSON(value):"object")::variant AS object,
    (PARSE_JSON(value):"offer")::variant AS offer,
    (PARSE_JSON(value):"offsite_pixel")::variant AS offsite_pixel,
    (PARSE_JSON(value):"page")::variant AS page,
    (PARSE_JSON(value):"post")::variant AS post,
    (PARSE_JSON(value):"question")::variant AS question,
    (PARSE_JSON(value):"response")::variant AS response,
    (PARSE_JSON(value):"subtype")::variant AS subtype,
    (PARSE_JSON(value):"action.type")::variant AS action_type,
    (PARSE_JSON(value):"event_creator")::variant AS event_creator,
    (PARSE_JSON(value):"object_domain")::variant AS object_domain,
    (PARSE_JSON(value):"offer_creator")::variant AS offer_creator,
    (PARSE_JSON(value):"page_parent")::variant AS page_parent,
    (PARSE_JSON(value):"post_object_wall")::variant AS post_object_wall,
    (PARSE_JSON(value):"post_wall")::variant AS post_wall,
    (PARSE_JSON(value):"post_object")::variant AS post_object,
    (PARSE_JSON(value):"question_creator")::variant AS question_creator,
    (PARSE_JSON(value):"conversion_id")::variant AS conversion_id,
    _sdc_batched_at

FROM {{ source('tap_facebook', 'ads') }},
    LATERAL SPLIT_TO_TABLE(
        input => ARRAY_TO_STRING(PARSE_JSON(conversion_specs), '|'), '|'
    )
