SELECT account_id,
      
      business_country_code,
       --MIN_REACH_LIMITS,
       --MAX_DAYS_TO_FINISH, 
       --MIN_CAMPAIGN_DURATION,
       --MAX_CAMPAIGN_DURATION
      _sdc_batched_at
FROM {{ source('tap_facebook', 'adaccounts') }} as reach_frequency