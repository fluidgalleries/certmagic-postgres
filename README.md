# certmagic-postgres


This plugin expects the following tables exist in the configured postgres database:
```
create table if not exists certmagic_data (
    key text primary key,
    value bytea,
    modified timestamptz default current_timestamp
)

create table if not exists certmagic_locks (
    key text primary key,
    expires timestamptz default current_timestamp
)
```