create table if not exists PERSON(id integer not null, first_name varchar(50), last_name varchar(50), PRIMARY KEY(id));

delete from PERSON;

insert into PERSON(id, first_name, last_name) values(1, 'Johannes', 'Kepler');
insert into PERSON(id, first_name, last_name) values(2, 'Galileo', 'Galilei');
insert into PERSON(id, first_name, last_name) values(3, 'Henry', 'More');
insert into PERSON(id, first_name, last_name) values(4, 'Polish', 'Brethren');
insert into PERSON(id, first_name, last_name) values(5, 'Robert', 'Boyle');
insert into PERSON(id, first_name, last_name) values(6, 'Isaac', 'Newton');