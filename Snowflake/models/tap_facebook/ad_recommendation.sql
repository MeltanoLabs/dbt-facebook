SELECT id as ad_id,
       TO_TIMESTAMP_NTZ(updated_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as ad_updated_time,
       PARSE_JSON(ARRAY_TO_STRING(PARSE_JSON(recommendations), '')):code::number as code,
       PARSE_JSON(ARRAY_TO_STRING(PARSE_JSON(recommendations), '')):blame_field::varchar as blame_field,
       PARSE_JSON(ARRAY_TO_STRING(PARSE_JSON(recommendations), '')):confidence::varchar as confidence,
       PARSE_JSON(ARRAY_TO_STRING(PARSE_JSON(recommendations), '')):importance::varchar as importance,
       PARSE_JSON(ARRAY_TO_STRING(PARSE_JSON(recommendations), '')):message::varchar as message,
       PARSE_JSON(ARRAY_TO_STRING(PARSE_JSON(recommendations), '')):title::varchar as title,
       recommendation_data,
       _sdc_batched_at

FROM {{ source('tap_facebook', 'ads') }}