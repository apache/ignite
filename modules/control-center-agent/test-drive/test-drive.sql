/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

CREATE TABLE COUNTRY (
    ID           INTEGER NOT NULL PRIMARY KEY,
    COUNTRY_NAME VARCHAR(100)
);

CREATE TABLE DEPARTMENT (
    DEPARTMENT_ID   INTEGER     NOT NULL PRIMARY KEY,
    DEPARTMENT_NAME VARCHAR(50) NOT NULL,
    COUNTRY_ID      INTEGER,
    MANAGER_ID      INTEGER
);

CREATE TABLE EMPLOYEE (
    EMPLOYEE_ID   INTEGER     NOT NULL PRIMARY KEY,
    FIRST_NAME    VARCHAR(20) NOT NULL,
    LAST_NAME     VARCHAR(30) NOT NULL,
    EMAIL         VARCHAR(25) NOT NULL,
    PHONE_NUMBER  VARCHAR(20),
    HIRE_DATE     DATE        NOT NULL,
    JOB           VARCHAR(50) NOT NULL,
    SALARY        DOUBLE,
    MANAGER_ID    INTEGER,
    DEPARTMENT_ID INTEGER
);

CREATE INDEX EMP_SALARY_A ON EMPLOYEE (SALARY ASC);
CREATE INDEX EMP_SALARY_B ON EMPLOYEE (SALARY DESC);
CREATE INDEX EMP_NAMES ON EMPLOYEE (FIRST_NAME ASC, LAST_NAME ASC);

CREATE SCHEMA CARS;

CREATE TABLE CARS.PARKING (
    PARKING_ID   INTEGER     NOT NULL PRIMARY KEY,
    PARKING_NAME VARCHAR(50) NOT NULL
);

CREATE TABLE CARS.CAR (
    CAR_ID     INTEGER     NOT NULL PRIMARY KEY,
    PARKING_ID INTEGER     NOT NULL,
    CAR_NAME   VARCHAR(50) NOT NULL
);
