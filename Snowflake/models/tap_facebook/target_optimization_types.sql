SELECT id,
       updated_time,
       NULL as index, --add data for INDEX
      --VALUE
      --KEY
       _sdc_batched_at
FROM {{ source('tap_facebook', 'adsets') }}