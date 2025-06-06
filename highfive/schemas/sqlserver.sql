create table quadrant (
  seq int not null,
  region int not null,
  name varchar(20),
  planned_for date,
  created_at datetime2,
  primary key (region, seq)
);

insert into quadrant (seq, region, name, planned_for, created_at) values
  (1, 100, 'North', '2023-12-15', '2023-12-22 12:34:56'),
  (1, 101, 'NE', '2023-10-20', '2023-12-22 12:34:56'),
  (1, 102, 'NW', '2023-09-20', '2023-12-22 12:34:56'),
  (1, 103, 'EES', '2023-08-20', '2023-12-22 12:34:56');

create table kitchen (
  id int primary key not null,
  name varchar(20)
);

insert into kitchen (id, name) values
  (101, 'Anne'),
  (102, 'Peter'),
  (103, 'Mary'),
  (104, 'Lucas');

create table data (
  id int primary key not null,
  i int,
  n1 decimal(10, 2),
  n2 bigint,
  d date,
  dt datetime2,
  c char(3),
  v varchar(max)
);

insert into data (id, i, n1, n2, d, dt, c, v) values
  (1, 12345678, 123456.78, 123456789012345678, '2024-08-14', '2024-08-15 12:34:56', 'ACT', 'This is a very long text with " and '' and <''> too.'),
  (2, null,     null,      null,               null,         null,                  null, null),
  (3, -12345678, -123456.78, -123456789012345678, '1960-08-14', '2024-08-15 12:34:56', 'a''b', 'Text with " and '' and <''> </> <\> too.');
  
create table "CASE" (
  region int not null,
  sector int,
  name varchar(20),
  unique (region, sector)
);

insert into "CASE" (region, sector, name) values (123, 456, 'Chicago');
insert into "CASE" (region, sector, name) values (140, 70, 'LA');
insert into "CASE" (region, sector, name) values (140, 71, 'Denver');
