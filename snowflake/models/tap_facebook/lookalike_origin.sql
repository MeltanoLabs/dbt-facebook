SELECT ID AS CUSTOM_AUDIENCE_ID,
       CAST(TIME_UPDATED as datetime) as CUSTOM_AUDIENCE_UPDATED_TIME,
        NULL as INDEX, --add data for INDEX
        NAME,
        --TYPE
        --ORIGIN_CUSTOM_AUDIENCE_ID
       _SDC_BATCHED_AT

FROM {{ source('tap_facebook', 'customaudiences') }} as meltano_custom_audiences