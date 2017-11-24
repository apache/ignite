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

CREATE TABLE COUNTRY (
    ID         INTEGER NOT NULL PRIMARY KEY,
    NAME       VARCHAR(50),
    POPULATION INTEGER NOT NULL
);

CREATE TABLE DEPARTMENT (
    ID         INTEGER NOT NULL PRIMARY KEY,
    COUNTRY_ID INTEGER NOT NULL,
    NAME       VARCHAR(50) NOT NULL
);

CREATE TABLE EMPLOYEE (
    ID            INTEGER NOT NULL PRIMARY KEY,
    DEPARTMENT_ID INTEGER NOT NULL,
    MANAGER_ID    INTEGER,
    FIRST_NAME    VARCHAR(50) NOT NULL,
    LAST_NAME     VARCHAR(50) NOT NULL,
    EMAIL         VARCHAR(50) NOT NULL,
    PHONE_NUMBER  VARCHAR(50),
    HIRE_DATE     DATE        NOT NULL,
    JOB           VARCHAR(50) NOT NULL,
    SALARY        DOUBLE
);

CREATE INDEX EMP_SALARY ON EMPLOYEE (SALARY ASC);
CREATE INDEX EMP_NAMES ON EMPLOYEE (FIRST_NAME ASC, LAST_NAME ASC);

CREATE SCHEMA CARS;

CREATE TABLE CARS.PARKING (
    ID       INTEGER     NOT NULL PRIMARY KEY,
    NAME     VARCHAR(50) NOT NULL,
    CAPACITY INTEGER NOT NULL
);

CREATE TABLE CARS.CAR (
    ID         INTEGER NOT NULL PRIMARY KEY,
    PARKING_ID INTEGER NOT NULL,
    NAME       VARCHAR(50) NOT NULL
);

INSERT INTO COUNTRY(ID, NAME, POPULATION) VALUES(0, 'Country #1', 10000000);
INSERT INTO COUNTRY(ID, NAME, POPULATION) VALUES(1, 'Country #2', 20000000);
INSERT INTO COUNTRY(ID, NAME, POPULATION) VALUES(2, 'Country #3', 30000000);

INSERT INTO DEPARTMENT(ID, COUNTRY_ID, NAME) VALUES(0, 0, 'Department #1');
INSERT INTO DEPARTMENT(ID, COUNTRY_ID, NAME) VALUES(1, 0, 'Department #2');
INSERT INTO DEPARTMENT(ID, COUNTRY_ID, NAME) VALUES(2, 2, 'Department #3');
INSERT INTO DEPARTMENT(ID, COUNTRY_ID, NAME) VALUES(3, 1, 'Department #4');
INSERT INTO DEPARTMENT(ID, COUNTRY_ID, NAME) VALUES(4, 1, 'Department #5');
INSERT INTO DEPARTMENT(ID, COUNTRY_ID, NAME) VALUES(5, 1, 'Department #6');

INSERT INTO EMPLOYEE(ID, DEPARTMENT_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER, HIRE_DATE, JOB, SALARY) VALUES(0, 0, 'First name manager #1', 'Last name manager #1', 'Email manager #1', 'Phone number manager #1', '2014-01-01', 'Job manager #1', 1100.00);
INSERT INTO EMPLOYEE(ID, DEPARTMENT_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER, HIRE_DATE, JOB, SALARY) VALUES(1, 1, 'First name manager #2', 'Last name manager #2', 'Email manager #2', 'Phone number manager #2', '2014-01-01', 'Job manager #2', 2100.00);
INSERT INTO EMPLOYEE(ID, DEPARTMENT_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER, HIRE_DATE, JOB, SALARY) VALUES(2, 2, 'First name manager #3', 'Last name manager #3', 'Email manager #3', 'Phone number manager #3', '2014-01-01', 'Job manager #3', 3100.00);
INSERT INTO EMPLOYEE(ID, DEPARTMENT_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER, HIRE_DATE, JOB, SALARY) VALUES(3, 3, 'First name manager #4', 'Last name manager #4', 'Email manager #4', 'Phone number manager #4', '2014-01-01', 'Job manager #4', 1500.00);
INSERT INTO EMPLOYEE(ID, DEPARTMENT_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER, HIRE_DATE, JOB, SALARY) VALUES(4, 4, 'First name manager #5', 'Last name manager #5', 'Email manager #5', 'Phone number manager #5', '2014-01-01', 'Job manager #5', 1700.00);
INSERT INTO EMPLOYEE(ID, DEPARTMENT_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER, HIRE_DATE, JOB, SALARY) VALUES(5, 5, 'First name manager #6', 'Last name manager #6', 'Email manager #6', 'Phone number manager #6', '2014-01-01', 'Job manager #6', 1300.00);

INSERT INTO EMPLOYEE(ID, DEPARTMENT_ID, MANAGER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER, HIRE_DATE, JOB, SALARY) VALUES(101, 0, 0, 'First name employee #1', 'Last name employee #1', 'Email employee #1', 'Phone number employee #1', '2014-01-01', 'Job employee #1', 600.00);
INSERT INTO EMPLOYEE(ID, DEPARTMENT_ID, MANAGER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER, HIRE_DATE, JOB, SALARY) VALUES(102, 0, 0, 'First name employee #2', 'Last name employee #2', 'Email employee #2', 'Phone number employee #2', '2014-01-01', 'Job employee #2', 1600.00);
INSERT INTO EMPLOYEE(ID, DEPARTMENT_ID, MANAGER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER, HIRE_DATE, JOB, SALARY) VALUES(103, 1, 1, 'First name employee #3', 'Last name employee #3', 'Email employee #3', 'Phone number employee #3', '2014-01-01', 'Job employee #3', 2600.00);
INSERT INTO EMPLOYEE(ID, DEPARTMENT_ID, MANAGER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER, HIRE_DATE, JOB, SALARY) VALUES(104, 2, 2, 'First name employee #4', 'Last name employee #4', 'Email employee #4', 'Phone number employee #4', '2014-01-01', 'Job employee #4', 1000.00);
INSERT INTO EMPLOYEE(ID, DEPARTMENT_ID, MANAGER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER, HIRE_DATE, JOB, SALARY) VALUES(105, 2, 2, 'First name employee #5', 'Last name employee #5', 'Email employee #5', 'Phone number employee #5', '2014-01-01', 'Job employee #5', 1200.00);
INSERT INTO EMPLOYEE(ID, DEPARTMENT_ID, MANAGER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER, HIRE_DATE, JOB, SALARY) VALUES(106, 2, 2, 'First name employee #6', 'Last name employee #6', 'Email employee #6', 'Phone number employee #6', '2014-01-01', 'Job employee #6', 800.00);
INSERT INTO EMPLOYEE(ID, DEPARTMENT_ID, MANAGER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER, HIRE_DATE, JOB, SALARY) VALUES(107, 3, 3, 'First name employee #7', 'Last name employee #7', 'Email employee #7', 'Phone number employee #7', '2014-01-01', 'Job employee #7', 1400.00);
INSERT INTO EMPLOYEE(ID, DEPARTMENT_ID, MANAGER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER, HIRE_DATE, JOB, SALARY) VALUES(108, 4, 4, 'First name employee #8', 'Last name employee #8', 'Email employee #8', 'Phone number employee #8', '2014-01-01', 'Job employee #8', 800.00);
INSERT INTO EMPLOYEE(ID, DEPARTMENT_ID, MANAGER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER, HIRE_DATE, JOB, SALARY) VALUES(109, 4, 4, 'First name employee #9', 'Last name employee #9', 'Email employee #9', 'Phone number employee #9', '2014-01-01', 'Job employee #9', 1490.00);
INSERT INTO EMPLOYEE(ID, DEPARTMENT_ID, MANAGER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER, HIRE_DATE, JOB, SALARY) VALUES(110, 4, 4, 'First name employee #10', 'Last name employee #12', 'Email employee #10', 'Phone number employee #10', '2014-01-01', 'Job employee #10', 1600.00);
INSERT INTO EMPLOYEE(ID, DEPARTMENT_ID, MANAGER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER, HIRE_DATE, JOB, SALARY) VALUES(111, 5, 5, 'First name employee #11', 'Last name employee #11', 'Email employee #11', 'Phone number employee #11', '2014-01-01', 'Job employee #11', 400.00);

INSERT INTO CARS.PARKING(ID, NAME, CAPACITY) VALUES(0, 'Parking #1', 10);
INSERT INTO CARS.PARKING(ID, NAME, CAPACITY) VALUES(1, 'Parking #2', 20);
INSERT INTO CARS.PARKING(ID, NAME, CAPACITY) VALUES(2, 'Parking #3', 30);

INSERT INTO CARS.CAR(ID, PARKING_ID, NAME) VALUES(0, 0, 'Car #1');
INSERT INTO CARS.CAR(ID, PARKING_ID, NAME) VALUES(1, 0, 'Car #2');
INSERT INTO CARS.CAR(ID, PARKING_ID, NAME) VALUES(2, 0, 'Car #3');
INSERT INTO CARS.CAR(ID, PARKING_ID, NAME) VALUES(3, 1, 'Car #4');
INSERT INTO CARS.CAR(ID, PARKING_ID, NAME) VALUES(4, 1, 'Car #5');
INSERT INTO CARS.CAR(ID, PARKING_ID, NAME) VALUES(5, 2, 'Car #6');
INSERT INTO CARS.CAR(ID, PARKING_ID, NAME) VALUES(6, 2, 'Car #7');
INSERT INTO CARS.CAR(ID, PARKING_ID, NAME) VALUES(7, 2, 'Car #8');
INSERT INTO CARS.CAR(ID, PARKING_ID, NAME) VALUES(8, 2, 'Car #9');
