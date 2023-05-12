SELECT id as ad_set_id,
       TO_TIMESTAMP_NTZ(updated_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as ad_set_updated_time,
       index-1 as index,
       billing_event as event,
       {{ var("frequency_control")["interval_days"]}} as interval_days,
       {{ var("frequency_control")["max_frequency"]}} as max_frequency,
       _sdc_batched_at
       
FROM {{ source('tap_facebook', 'adsets') }},
lateral split_to_table(input=>billing_event, '|') BILLING_EVENT_COLUMN
