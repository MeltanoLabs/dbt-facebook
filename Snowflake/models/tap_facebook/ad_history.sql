{{
   config(
     materialized='view'
   )
}}
 
SELECT ID::number as ID,
       TO_TIMESTAMP_NTZ(UPDATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as UPDATED_TIME,
       ACCOUNT_ID::number as ACCOUNT_ID,
       CAMPAIGN_ID::number as CAMPAIGN_ID,
       CREATIVE:id::number as CREATIVE_ID,
       BID_AMOUNT::number as BID_AMOUNT,
       BID_TYPE,
       CONFIGURED_STATUS,
       CONVERSION_DOMAIN,
       TO_TIMESTAMP_NTZ(CREATED_TIME, 'YYYY-MM-DD"T"HH24:MI:SSTZHTZM') as CREATED_TIME,
       EFFECTIVE_STATUS,
       LAST_UPDATED_BY_APP_ID:: number as LAST_UPDATED_BY_APP_ID,
       NAME,
       STATUS,
       ADSET_ID::number as AD_SET_ID,
       SOURCE_AD_ID::number as AD_SOURCE_ID,
       BID_INFO:ACTIONS::number as BID_INFO_ACTIONS,
       PLACEMENT_SPECIFIC_FACEBOOK_UNSAFE_SUBSTANCES,
       PLACEMENT_SPECIFIC_INSTAGRAM_UNSAFE_SUBSTANCES,
       GLOBAL_UNSAFE_SUBSTANCES,
       PLACEMENT_SPECIFIC_INSTAGRAM_PERSONAL_ATTRIBUTES,
       GLOBAL_PERSONAL_ATTRIBUTES,
       PLACEMENT_SPECIFIC_FACEBOOK_PERSONAL_ATTRIBUTES,
       PLACEMENT_SPECIFIC_INSTAGRAM_NONEXISTENT_FUNCTIONALITY,
       GLOBAL_NONEXISTENT_FUNCTIONALITY,
       PLACEMENT_SPECIFIC_FACEBOOK_NONEXISTENT_FUNCTIONALITY,
       PLACEMENT_SPECIFIC_FACEBOOK_ADVERTISING_POLICIES,
       PLACEMENT_SPECIFIC_INSTAGRAM_ADVERTISING_POLICIES,
       GLOBAL_ADVERTISING_POLICIES,
       GLOBAL_SPYWARE_OR_MALWARE,
       PLACEMENT_SPECIFIC_INSTAGRAM_SPYWARE_OR_MALWARE,
       PLACEMENT_SPECIFIC_FACEBOOK_SPYWARE_OR_MALWARE,
       PLACEMENT_SPECIFIC_INSTAGRAM_UNREALISTIC_OUTCOMES,
       GLOBAL_UNREALISTIC_OUTCOMES,
       PLACEMENT_SPECIFIC_FACEBOOK_UNREALISTIC_OUTCOMES,
       PLACEMENT_SPECIFIC_FACEBOOK_BRAND_USAGE_IN_ADS,
       GLOBAL_BRAND_USAGE_IN_ADS,
       GLOBAL_PERSONAL_HEALTH_AND_APPEARANCE,
       PLACEMENT_SPECIFIC_FACEBOOK_PERSONAL_HEALTH_AND_APPEARANCE,
       PLACEMENT_SPECIFIC_INSTAGRAM_PERSONAL_HEALTH_AND_APPEARANCE,
       PLACEMENT_SPECIFIC_INSTAGRAM_ILLEGAL_PRODUCTS_OR_SERVICES,
       GLOBAL_ILLEGAL_PRODUCTS_OR_SERVICES,
       PLACEMENT_SPECIFIC_FACEBOOK_ILLEGAL_PRODUCTS_OR_SERVICES,
       GLOBAL_NON_FUNCTIONAL_LANDING_PAGE,
       PLACEMENT_SPECIFIC_FACEBOOK_NON_FUNCTIONAL_LANDING_PAGE,
       PLACEMENT_SPECIFIC_INSTAGRAM_NON_FUNCTIONAL_LANDING_PAGE,
       PLACEMENT_SPECIFIC_INSTAGRAM_COMMERCIAL_EXPLOITATION_OF_CRISES_AND_CONTROVERSIAL_EVENTS,
       PLACEMENT_SPECIFIC_FACEBOOK_COMMERCIAL_EXPLOITATION_OF_CRISES_AND_CONTROVERSIAL_EVENTS,
       GLOBAL_COMMERCIAL_EXPLOITATION_OF_CRISES_AND_CONTROVERSIAL_EVENTS,
       GLOBAL_DISCRIMINATORY_PRACTICES,
       PLACEMENT_SPECIFIC_FACEBOOK_DISCRIMINATORY_PRACTICES,
       GLOBAL_CIRCUMVENTING_SYSTEMS,
       PLACEMENT_SPECIFIC_FACEBOOK_CIRCUMVENTING_SYSTEMS,
       PLACEMENT_SPECIFIC_INSTAGRAM_CIRCUMVENTING_SYSTEMS,
       PLACEMENT_SPECIFIC_FACEBOOK_ADULT_CONTENT,
       PLACEMENT_SPECIFIC_FACEBOOK_SENSATIONAL_CONTENT,
       GLOBAL_ADULT_CONTENT,
       BID_INFO:impressions::number as BID_INFO_IMPRESSIONS,
       GLOBAL_SENSATIONAL_CONTENT,
       PLACEMENT_SPECIFIC_INSTAGRAM_ADULT_CONTENT,
       PLACEMENT_SPECIFIC_INSTAGRAM_BRAND_USAGE_IN_ADS,
       PLACEMENT_SPECIFIC_INSTAGRAM_SENSATIONAL_CONTENT,
       PLACEMENT_SPECIFIC_FACEBOOK_ADS_ABOUT_SOCIAL_ISSUES_ELECTIONS_OR_POLITICS,
       PLACEMENT_SPECIFIC_INSTAGRAM_ADS_ABOUT_SOCIAL_ISSUES_ELECTIONS_OR_POLITICS, 
       GLOBAL_ADS_ABOUT_SOCIAL_ISSUES_ELECTIONS_OR_POLITICS,
       _SDC_BATCHED_AT

FROM {{ source('tap_facebook', 'ads') }} as ad_history
