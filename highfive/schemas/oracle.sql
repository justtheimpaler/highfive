create table quadrant (
  seq number(9) not null,
  region number(9) not null,
  name varchar2(20),
  planned_for date,
  created_at date,
  primary key (region, seq)
);

insert into quadrant (seq, region, name, planned_for, created_at) values
  (1, 100, 'North', date '2023-12-15', timestamp '2023-12-22 12:34:56');
  
insert into quadrant (seq, region, name, planned_for, created_at) values
  (1, 101, 'NE', date '2023-12-20', timestamp '2023-12-22 12:34:56');

create table kitchen (
  id number(9) primary key not null,
  name varchar2(20)
);

insert into kitchen (id, name) values (101, 'Anne');

insert into kitchen (id, name) values (102, 'Peter');

create table data (
  id number(8) primary key not null,
  i number(8),
  n1 decimal(10, 2),
  n2 number(18),
  d date,
  dt timestamp,
  c char(3),
  v varchar2(4000)
);

insert into data (id, i, n1, n2, d, dt, c, v) values
  (1, 12345678, 123456.78, 123456789012345678, date '2024-08-14', timestamp '2024-08-15 12:34:56', 'ACT', 'This is a very long text with " and '' and <''> too.');

insert into data (id, i, n1, n2, d, dt, c, v) values
  (2, null,     null,      null,               null,         null,                  null, null);

insert into data (id, i, n1, n2, d, dt, c, v) values
  (3, -12345678, -123456.78, -123456789012345678, date '1960-08-14', timestamp '2024-08-15 12:34:56', 'a''b', 'Text with " and '' and <''> </> <\> too.');

