SELECT IMAGE_HASH as HASH,
       ID,
       --MISSING: UPDATED_TIME
       ACCOUNT_ID,
       -- MISSING: CREATED_TIME
       -- MISSING: CREATIVES
       -- MISSING: HEIGHT
       -- MISSING: IS_ASSOCIATED_IN_ADGROUPS
       NAME,
       INSTAGRAM_PERMALINK_URL AS PERMALINK_URL,
       STATUS,
       URL,
       -- MISSING: WIDTH


FROM RYAN_MIRANDA_RAW.MELTANO_FACEBOOK.CREATIVES;