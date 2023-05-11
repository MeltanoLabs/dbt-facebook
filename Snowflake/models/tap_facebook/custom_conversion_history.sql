SELECT id,
       account_id,
       name,
       _sdc_batched_at

FROM {{ source('tap_facebook', 'customconversions') }} as meltano_custom_conversion_history