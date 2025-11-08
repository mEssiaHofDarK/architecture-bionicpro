create extension if not exists "uuid-ossp";
create table if not exists users(
    id UUID default uuid_generate_v4() primary key,
    user_id UUID,
    name text
);

insert into users (user_id, name) values
('f5e2e99d-6ff7-4cc6-a2e5-9181b6b5f8c0', 'prothetic1'),
('4ad5936e-72b8-4788-afa6-55695ce4fa00', 'prothetic2'),
('7b46949c-4837-4b7f-abf3-1d74d5afd13c', 'prothetic3')
