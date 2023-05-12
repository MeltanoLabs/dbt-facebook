SELECT account_id,
       adlabels,
       TO_TIMESTAMP_NTZ(created_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as created_time,
       --ERROR_CODE,
       --ERROR_MESSAGE,
       --ERROR_SUMMARY,
       --ERROR_TYPE,
       id,
       NULL as index, --Add value for INDEX
       --LEVEL,
       --PROMOTED_OBJECT,
       TO_TIMESTAMP_NTZ(updated_time, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as updated_time,
       _sdc_batched_at

FROM {{ source('tap_facebook', 'ads') }}