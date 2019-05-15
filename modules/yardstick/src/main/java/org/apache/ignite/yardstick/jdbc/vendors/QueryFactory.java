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

package org.apache.ignite.yardstick.jdbc.vendors;

/**
 * Creates queries. Currently queries are written in SQL dialect that is common to Ignite SQL, MySql and PostgreSQL.
 */
public class QueryFactory {
    /** Query that creates Person table. */
    public String createPersonTab() {
        return "CREATE TABLE PUBLIC.PERSON (" +
            "id BIGINT PRIMARY KEY, " +
            "org_id BIGINT, " +
            "first_name VARCHAR(255), " +
            "last_name VARCHAR(255), " +
            "salary BIGINT);";
    }

    /** Query that creates Organization table. */
    public String createOrgTab() {
        return "CREATE TABLE PUBLIC.ORGANIZATION (id BIGINT PRIMARY KEY, name VARCHAR(255));";
    }

    /** Query that creates index on 'salary' field. */
    public String createSalaryIdx() {
        return "CREATE INDEX sal_idx ON PUBLIC.PERSON(salary);";
    }

    /** Query that creates index on Person.org_id to have fast join query. */
    public String createOrgIdIdx() {
        return "CREATE INDEX org_id_idx ON PUBLIC.PERSON(org_id)";
    }

    /** Query that drops Person table. */
    public String dropPersonIfExist() {
        return "DROP TABLE IF EXISTS PUBLIC.PERSON;";
    }

    /** Query that drops Person table. */
    public String dropOrgIfExist() {
        return "DROP TABLE IF EXISTS PUBLIC.ORGANIZATION;";
    }

    /** Query to execute before data upload. */
    public String beforeLoad() {
        return null;
    }

    /** Query to execute after data upload. */
    public String afterLoad() {
        return null;
    }

    /**
     * Query that fetches persons which salaries are in range. Range borders are specified as parameters of
     * PreparedStatement.
     */
    public String selectPersonsWithSalaryBetween() {
        return "SELECT * FROM PUBLIC.PERSON WHERE SALARY BETWEEN ? AND ?";
    }

    /** Simple select query that fetches person with specified Person.id. */
    public String selectPersonsByPK() {
        return "SELECT * FROM PUBLIC.PERSON WHERE id = ? ;";
    }

    /** Query that inserts new Person record. Has 5 jdbc parameters - fields of the Person. */
    public String insertIntoPerson() {
        return "INSERT INTO PUBLIC.PERSON (id, org_id, first_name, last_name, salary) values (?, ?, ?, ?, ?)";
    }

    /** Query that inserts new Organization record. Has 2 jdbc parameters - org id and org name. */
    public String insertIntoOrganization() {
        return "INSERT INTO PUBLIC.ORGANIZATION (id, name) VALUES (?, ?);";
    }

    /**
     * Query that fetches info about persons and theirs organizations for that persons who has salary in specified
     * range.
     */
    public String selectPersonsJoinOrgWhereSalary() {
        return "SELECT p.id, p.org_id, p.first_name, p.last_name, p.salary, o.name " +
            "FROM PUBLIC.PERSON p " +
            "INNER JOIN PUBLIC.ORGANIZATION o " +
            "ON p.org_id = o.id " +
            "WHERE salary BETWEEN ? AND ?;";
    }

    /**
     * Query that fetches info about person with specified Person.id and it's organization.
     */
    public String selectPersonsJoinOrgWherePersonPK() {
        return "SELECT p.id, p.org_id, p.first_name, p.last_name, p.salary, o.name " +
            "FROM PUBLIC.PERSON p " +
            "INNER JOIN PUBLIC.ORGANIZATION o " +
            "ON p.org_id = o.id " +
            "WHERE p.id = ?;";
    }

    /** Query that fetches all ids from Person table. Has no parameters. */
    public String selectAllPersons() {
        return "SELECT * FROM PUBLIC.PERSON;";
    }

    /** Query that fetches all records about all persons with info about theirs organizations. */
    public String selectAllPersonsJoinOrg() {
        return "SELECT p.id, p.org_id, p.first_name, p.last_name, p.salary, o.name " +
            "FROM PUBLIC.PERSON p " +
            "INNER JOIN PUBLIC.ORGANIZATION o " +
            "ON p.org_id = o.id;";
    }
}
