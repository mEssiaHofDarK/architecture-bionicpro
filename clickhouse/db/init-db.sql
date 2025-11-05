create database main_db;
create table if not exists reports(
    id UUID default generateUUIDv4(),
    user_id UUID,
    name String,
    telemetry String,
    date DateTime
) engine = MergeTree()
order by (user_id, date);
