SELECT
    id AS ad_id,
    TO_TIMESTAMP_NTZ(
        updated_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM'
    ) AS ad_updated_time,
    PARSE_JSON(
        ARRAY_TO_STRING(PARSE_JSON(recommendations), '')
    ):code::number AS code,
    PARSE_JSON(
        ARRAY_TO_STRING(PARSE_JSON(recommendations), '')
    ):blame_field::varchar AS blame_field,
    PARSE_JSON(
        ARRAY_TO_STRING(PARSE_JSON(recommendations), '')
    ):confidence::varchar AS confidence,
    PARSE_JSON(
        ARRAY_TO_STRING(PARSE_JSON(recommendations), '')
    ):importance::varchar AS importance,
    PARSE_JSON(
        ARRAY_TO_STRING(PARSE_JSON(recommendations), '')
    ):message::varchar AS message,
    PARSE_JSON(
        ARRAY_TO_STRING(PARSE_JSON(recommendations), '')
    ):title::varchar AS title,
    recommendation_data,
    _sdc_batched_at

FROM {{ source('tap_facebook', 'ads') }}
