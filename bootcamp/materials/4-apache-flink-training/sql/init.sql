-- Create processed_events table
CREATE TABLE IF NOT EXISTS processed_events (
    ip VARCHAR,
    event_timestamp TIMESTAMP(3),
    referrer VARCHAR,
    host VARCHAR,
    url VARCHAR,
    geodata VARCHAR
);

CREATE TABLE processed_events_aggregated (
            event_hour TIMESTAMP(3),
            host VARCHAR,
            num_hits BIGINT
        )

CREATE TABLE processed_events_aggregated_source(
            event_hour TIMESTAMP(3),
            host VARCHAR,
            referrer VARCHAR,
            num_hits BIGINT
)

select * from processed_events_aggregated;
select * from processed_events_aggregated_source;
--delete from processed_events

select geodata::json->>'country',
    count(1)
    from processed_events
    group by 1
