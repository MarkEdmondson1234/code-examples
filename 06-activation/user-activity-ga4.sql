SELECT
    timestamp_micros(event_timestamp) as event_timestamp,
    user_pseudo_id ,
    event_name,
    TIMESTAMP_MICROS(user_first_touch_timestamp) as first_touch,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') as page_location,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_referrer') as page_referrer
FROM
     `mark-edmondson-gde.analytics_206670707.events_*`
WHERE
    _table_suffix between format_date('%%Y%%m%%d',date_sub(current_date(), interval 90 day)) and format_date('%%Y%%m%%d',date_sub(current_date(), interval 0 day))
    and event_name = 'page_view' 
    and user_pseudo_id = '%s'
ORDER BY event_timestamp