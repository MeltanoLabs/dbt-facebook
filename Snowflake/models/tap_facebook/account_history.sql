SELECT AGE,
       AMOUNT_SPENT,
       BALANCE,
       BUSINESS_CITY,
       BUSINESS_COUNTRY_CODE,
       BUSINESS_NAME,
       BUSINESS_STATE,
       BUSINESS_STREET,
       BUSINESS_STREET2,
       BUSINESS_ZIP,
       CAN_CREATE_BRAND_LIFT_STUDY,
       CREATED_TIME,
       CURRENCY,
       END_ADVERTISER,
       END_ADVERTISER_NAME, 
       HAS_ADVERTISER_OPTED_IN_ODAX,
       HAS_MIGRATED_PERMISSIONS,
       IO_NUMBER,
       IS_ATTRIBUTION_SPEC_SYSTEM_DEFAULT,
       IS_DIRECT_DEALS_ENABLED,
       IS_IN_3DS_AUTHORIZATION_ENABLED_MARKET,
       IS_NOTIFICATIONS_ENABLED,
       IS_PERSONAL,
       IS_PREPAY_ACCOUNT,
       IS_TAX_ID_REQUIRED,
       MEDIA_AGENCY,
       MIN_CAMPAIGN_GROUP_SPEND_CAP,
       MIN_DAILY_BUDGET,
       NAME,
       null as NEXT_BILL_DATE,
       OFFSITE_PIXELS_TOS_ACCEPTED,
       OWNER,
       PARTNER,
       SALESFORCE_INVOICE_GROUP_ID,
       SPEND_CAP,
       TAX_ID,
       TAX_ID_TYPE,
       TIMEZONE_ID,
       TIMEZONE_NAME,
       TIMEZONE_OFFSET_HOURS_UTC,
       ACCOUNT_STATUS,
       DISABLE_REASON, 
       TAX_ID_STATUS,
       AGENCY_CLIENT_DECLARATION_AGENCY_REPRESENTING_CLIENT,
       AGENCY_CLIENT_DECLARATION_CLIENT_BASED_IN_FRANCE,
       AGENCY_CLIENT_DECLARATION_CLIENT_CITY,
       AGENCY_CLIENT_DECLARATION_CLIENT_COUNTRY_CODE,
       AGENCY_CLIENT_DECLARATION_CLIENT_EMAIL_ADDRESS,
       AGENCY_CLIENT_DECLARATION_CLIENT_NAME,
       AGENCY_CLIENT_DECLARATION_CLIENT_POSTAL_CODE,
       AGENCY_CLIENT_DECLARATION_CLIENT_PROVINCE,
       AGENCY_CLIENT_DECLARATION_CLIENT_STREET,
       AGENCY_CLIENT_DECLARATION_CLIENT_STREET2,
       AGENCY_CLIENT_DECLARATION_HAS_WRITTEN_MANDATE_FROM_ADVERTISER,
       AGENCY_CLIENT_DECLARATION_IS_CLIENT_PAYING_INVOICES,
       BUSINESS_MANAGER_BLOCK_OFFLINE_ANALYTICS,
       BUSINESS_MANAGER_CREATED_BY,
       BUSINESS_MANAGER_CREATED_TIME,
       BUSINESS_MANAGER_EXTENDED_UPDATED_TIME,
       BUSINESS_MANAGER_IS_HIDDEN,
       BUSINESS_MANAGER_LINK,
       BUSINESS_MANAGER_NAME,
       BUSINESS_MANAGER_PAYMENT_ACCOUNT_ID,
       BUSINESS_MANAGER_PRIMARY_PAGE,
       BUSINESS_MANAGER_PROFILE_PICTURE_URI,
       BUSINESS_MANAGER_TIMEZONE_ID,
       BUSINESS_MANAGER_TWO_FACTOR_TYPE,
       BUSINESS_MANAGER_UPDATED_BY,
       BUSINESS_MANAGER_UPDATE_TIME,
       BUSINESS_MANAGER_VERIFICATION_STATUS,
       BUSINESS_MANAGER_VERTICAL,
       BUSINESS_MANAGER_VERTICAL_ID,
       BUSINESS_MANAGER_MANAGER_ID,
       EXTENDED_CREDIT_INVOICE_GROUP_ID,
       EXTENDED_CREDIT_INVOICE_GROUP_AUTO_ENROLL,
       EXTENDED_CREDIT_INVOICE_GROUP_CUSTOMER_PO_NUMBER,
       EXTENDED_CREDIT_INVOICE_GROUP_EMAIL,
       EXTENDED_CREDIT_INVOICE_GROUP_EMAILS,
       EXTENDED_CREDIT_INVOICE_GROUP_NAME,
       CAPABILITIES,
       ID,
       _SDC_BATCHED_AT

FROM {{ source('tap_facebook', 'adaccounts') }} as account_history