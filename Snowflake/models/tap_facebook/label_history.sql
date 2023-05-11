SELECT id,
       TO_TIMESTAMP_NTZ(updated_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as updated_time,
       account:account_id::number as account_id,
       TO_TIMESTAMP_NTZ(created_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as created_time,
       name,
       _sdc_batched_at       

FROM {{ source('tap_facebook', 'adlabels') }}