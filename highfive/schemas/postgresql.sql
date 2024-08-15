create table t1.quadrant (
  seq int not null,
  region int not null,
  name varchar(20),
  planned_for date,
  created_at timestamp,
  primary key (region, seq)
);

insert into t1.quadrant (seq, region, name, planned_for, created_at) values
  (1, 100, 'North', '2023-12-15', '2023-12-22 12:34:56'),
  (1, 101, 'NE', '2023-12-20', '2023-12-22 12:34:56');

create table t1.kitchen (
  id int primary key not null,
  name varchar(20)
);

insert into t1.kitchen (id, name) values
  (101, 'Anne'),
  (102, 'Peter');

-- Separate 't1' schema for tables without PK nor unique constraints/indexes.

create table t1.w (a int, b int, c int, d macaddr);

alter table t1.w add constraint wq1 unique (a, b);

create unique index ixt1 on t1.w (c, b, (a*2+b) desc);

create unique index ix2t1 on t1.w (b, c desc, a);

-- no non-unique
-- no collations
-- no nulls first/last
-- no partial indexes
-- no functional indexes
-- ASC/DESC is OK

insert into t1.w (a, b, c) values (1, 2, 3), (1, 3, 4), (1, 1, 2);

create table t1.x (a int, b int, c int);

insert into t1.x (a, b, c) values (1, 2, 3), (1, 3, 4), (1, 1, 2);


create table t1.data (
  id int primary key not null,
  i int,
  n1 decimal(10, 2),
  n2 bigint,
  d date,
  dt timestamp,
  c char(3),
  v text
);

insert into t1.data (id, i, n1, n2, d, dt, c, v) values
  (1, 12345678, 123456.78, 123456789012345678, '2024-08-14', '2024-08-15 12:34:56', 'ACT', 'This is a very long text with " and '' and <''> too.'),
  (2, null,     null,      null,               null,         null,                  null, null),
  (3, -12345678, -123456.78, -123456789012345678, '1960-08-14', '2024-08-15 12:34:56', 'a''b', 'Text with " and '' and <''> </> <\> too.');



