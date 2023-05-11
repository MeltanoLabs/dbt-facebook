SELECT id as ad_set_id,
       created_time as ad_set_created_time,
       updated_time as ad_set_updated_time,
       0 as index, --Add values for INDEX, MAX_FREQUENCY
       1 as max_frequency,--Add values for MAX_FREQUENCY
       7 as interval_days,--Add values for INTERVAL_DAYS
       billing_event as event,
       _sdc_batched_at
       
FROM {{ source('tap_facebook', 'adsets') }}