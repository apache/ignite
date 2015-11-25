--
--  Licensed to the Apache Software Foundation (ASF) under one or more
--  contributor license agreements.  See the NOTICE file distributed with
--  this work for additional information regarding copyright ownership.
--  The ASF licenses this file to You under the Apache License, Version 2.0
--  (the "License"); you may not use this file except in compliance with
--  the License.  You may obtain a copy of the License at
--
--       http://www.apache.org/licenses/LICENSE-2.0
--
--  Unless required by applicable law or agreed to in writing, software
--  distributed under the License is distributed on an "AS IS" BASIS,
--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  See the License for the specific language governing permissions and
--  limitations under the License.
--

-- Script of database initialization for Schema Import Demo.
drop table PERSON;

create table PERSON(id integer not null PRIMARY KEY, first_name varchar(50), last_name varchar(50), salary double not null);

insert into PERSON(id, first_name, last_name, salary) values(1, 'Johannes', 'Kepler', 1000);
insert into PERSON(id, first_name, last_name, salary) values(2, 'Galileo', 'Galilei', 2000);
insert into PERSON(id, first_name, last_name, salary) values(3, 'Henry', 'More', 3000);
insert into PERSON(id, first_name, last_name, salary) values(4, 'Polish', 'Brethren', 4000);
insert into PERSON(id, first_name, last_name, salary) values(5, 'Robert', 'Boyle', 5000);
insert into PERSON(id, first_name, last_name, salary) values(6, 'Isaac', 'Newton', 6000);
