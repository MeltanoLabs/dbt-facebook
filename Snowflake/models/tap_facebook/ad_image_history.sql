SELECT hash,
       id,
       updated_time,
       account_id,
       created_time,
       creatives,
       height,
       is_associated_creatives_in_adgroups as is_associated_in_adgroups,
       name,
       original_height,
       original_width,
       permalink_url,
       status,
       url,
       url_128,
       width,
       _sdc_batched_at


FROM ryan_miranda_raw.meltano_facebook.adimages