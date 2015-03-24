-- Script of database initialization for Schema Import Demo.
drop table PERSON;
create table PERSON(id integer not null, first_name varchar(50), last_name varchar(50), salary double not null, PRIMARY KEY(id));

insert into PERSON(id, first_name, last_name, salary) values(1, 'Johannes', 'Kepler', 1000);
insert into PERSON(id, first_name, last_name, salary) values(2, 'Galileo', 'Galilei', 2000);
insert into PERSON(id, first_name, last_name, salary) values(3, 'Henry', 'More', 3000);
insert into PERSON(id, first_name, last_name, salary) values(4, 'Polish', 'Brethren', 4000);
insert into PERSON(id, first_name, last_name, salary) values(5, 'Robert', 'Boyle', 5000);
insert into PERSON(id, first_name, last_name, salary) values(6, 'Isaac', 'Newton', 6000);
