SELECT id,
       TO_TIMESTAMP_NTZ(updated_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as updated_time,
       _sdc_batched_at
      --CUSTOM_AUDIENCE_ID, 
      --IS_EXCLUDED

FROM {{ source('tap_facebook', 'adsets') }} as meltano_ad_set_custom_audience