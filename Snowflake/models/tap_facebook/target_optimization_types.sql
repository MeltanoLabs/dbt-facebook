SELECT ID,
       UPDATED_TIME,
       NULL as INDEX, --add data for INDEX
      --VALUE
      --KEY
       _SDC_BATCHED_AT
FROM {{ source('tap_facebook', 'adsets') }} as meltano_ad_set_target_optimization