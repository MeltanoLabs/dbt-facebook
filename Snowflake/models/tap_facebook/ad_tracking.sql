SELECT
    id AS ad_id,
    TO_TIMESTAMP_NTZ(
        updated_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM'
    ) AS ad_updated_time,
    index - 1 AS index,
    application,
    CAST(PARSE_JSON(value):"conversion_id" AS variant) AS conversion_id,
    PARSE_JSON(creative):id::number AS creative,
    dataset,
    CAST(PARSE_JSON(value):"event" AS variant) AS event,
    CAST(PARSE_JSON(value):"event_type" AS variant) AS event_type,
    CAST(PARSE_JSON(value):"fb_pixel" AS variant) AS fb_pixel,
    CAST(PARSE_JSON(value):"fb_pixel_event" AS variant) AS fb_pixel_event,
    CAST(PARSE_JSON(value):"leadgen" AS variant) AS leadgen,
    CAST(PARSE_JSON(value):"object" AS variant) AS object,
    CAST(PARSE_JSON(value):"offer" AS variant) AS offer,
    CAST(PARSE_JSON(value):"offsite_pixel" AS variant) AS offsite_pixel,
    CAST(PARSE_JSON(value):"page" AS variant) AS page,
    CAST(PARSE_JSON(value):"post" AS variant) AS post,
    CAST(PARSE_JSON(value):"question" AS variant) AS question,
    CAST(PARSE_JSON(value):"response" AS variant) AS response,
    CAST(PARSE_JSON(value):"subtype" AS variant) AS subtype,
    CAST(PARSE_JSON(value):"action.type" AS variant) AS action_type,
    CAST(PARSE_JSON(value):"event_creator" AS variant) AS event_creator,
    CAST(PARSE_JSON(value):"object_domain" AS variant) AS object_domain,
    CAST(PARSE_JSON(value):"offer_creator" AS variant) AS offer_creator,
    CAST(PARSE_JSON(value):"page_parent" AS variant) AS page_parent,
    CAST(PARSE_JSON(value):"post_object_wall" AS variant) AS post_object_wall,
    CAST(PARSE_JSON(value):"post_wall" AS variant) AS post_wall,
    CAST(PARSE_JSON(value):"post_object" AS variant) AS post_object,
    CAST(PARSE_JSON(value):"question_creator" AS variant) AS question_creator,
    _sdc_batched_at

FROM {{ source('tap_facebook', 'ads') }},
    LATERAL SPLIT_TO_TABLE(
        input => ARRAY_TO_STRING(PARSE_JSON(tracking_specs), '|'), '|'
    )
