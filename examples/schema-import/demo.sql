--
-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

create table if not exists PERSON(id integer not null, first_name varchar(50), last_name varchar(50), PRIMARY KEY(id));

delete from PERSON;

insert into PERSON(id, first_name, last_name) values(1, 'Johannes', 'Kepler');
insert into PERSON(id, first_name, last_name) values(2, 'Galileo', 'Galilei');
insert into PERSON(id, first_name, last_name) values(3, 'Henry', 'More');
insert into PERSON(id, first_name, last_name) values(4, 'Polish', 'Brethren');
insert into PERSON(id, first_name, last_name) values(5, 'Robert', 'Boyle');
insert into PERSON(id, first_name, last_name) values(6, 'Isaac', 'Newton');