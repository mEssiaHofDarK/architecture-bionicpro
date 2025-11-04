create database main_db;
set allow_experimental_object_type = 1;
create table if not exists reports(
    id UUID default generateUUIDv4(),
    user_id UUID,
    name String,
    telemetry JSON,
    date DateTime
) engine = MergeTree()
order by (user_id, date);
