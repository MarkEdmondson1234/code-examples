SELECT
    -- event_date (the date on which the event was logged)
    parse_date('%Y%m%d',event_date) as event_date,
    -- event_timestamp (in microseconds, utc)
    timestamp_micros(event_timestamp) as event_timestamp,
    -- event_name (the name of the event)
    event_name,
    -- event_key (the event parameter's key)
    (SELECT key FROM UNNEST(event_params) WHERE key = 'page_location') as event_key,
    -- event_string_value (the string value of the event parameter)
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') as event_string_value
FROM
    -- your GA4 exports - change to your location
    `mark-edmondson-gde.analytics_206670707.events_*` 
WHERE
    -- limits query to use table from yesterday only
    _table_suffix = format_date('%Y%m%d',date_sub(current_date(), interval 1 day))
    and event_name = 'page_view' 