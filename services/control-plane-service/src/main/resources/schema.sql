create table if not exists sf_event (
  event_id varchar(64) primary key,
  idempotency_key varchar(160) unique not null,
  event_type varchar(120) not null,
  payload text not null,
  status varchar(40) not null,
  attempts integer not null default 0,
  replay_count integer not null default 0,
  processing_duration_ms bigint,
  last_error text,
  created_at timestamp with time zone not null default now(),
  updated_at timestamp with time zone not null default now(),
  processed_at timestamp with time zone
);
create index if not exists idx_sf_event_status on sf_event(status);
create index if not exists idx_sf_event_updated on sf_event(updated_at desc);
