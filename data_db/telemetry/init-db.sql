create extension if not exists "uuid-ossp";
create table if not exists telemetry(
    id UUID default uuid_generate_v4() primary key,
    user_id UUID,
    telemetry text,
    date timestamp default now()
);

insert into telemetry (user_id, telemetry, date) values
('f5e2e99d-6ff7-4cc6-a2e5-9181b6b5f8c0', 'some telemetry11', '2025-11-04'),
('f5e2e99d-6ff7-4cc6-a2e5-9181b6b5f8c0', 'some telemetry12', '2025-11-05'),
('f5e2e99d-6ff7-4cc6-a2e5-9181b6b5f8c0', 'some telemetry13', '2025-11-06'),
('4ad5936e-72b8-4788-afa6-55695ce4fa00', 'some telemetry21', '2025-11-04'),
('4ad5936e-72b8-4788-afa6-55695ce4fa00', 'some telemetry22', '2025-11-05'),
('4ad5936e-72b8-4788-afa6-55695ce4fa00', 'some telemetry23', '2025-11-06'),
('7b46949c-4837-4b7f-abf3-1d74d5afd13c', 'some telemetry31', '2025-11-04'),
('7b46949c-4837-4b7f-abf3-1d74d5afd13c', 'some telemetry32', '2025-11-05'),
('7b46949c-4837-4b7f-abf3-1d74d5afd13c', 'some telemetry33', '2025-11-06');
