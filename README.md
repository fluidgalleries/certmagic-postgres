# certmagic-postgres

### Description
Certmagic-postgres is a storage plugin for Caddy & Certmagic that allows data to be 
stored in a postgres database.

### Database Setup
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
Database migration files to create these tables can be found in the ```db``` directory. 

### Caddyfile

Inline configuration:
```
postgres postgres://localhost/mydatabase
```

Block configuration:
```
postgres {
    connection_string postgres://localhost/mydatabase
    query_timeout 3s
    lock_timeout 60s
}
```