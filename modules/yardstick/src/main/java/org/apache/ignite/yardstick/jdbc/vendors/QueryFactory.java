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
 * Creates queries.
 */
public class QueryFactory {
    /** Query that creates Person table. */
    public String createPersonTab() {
        return "CREATE TABLE PERSON (id LONG, first_name VARCHAR(255), last_name VARCHAR(255), salary LONG);";
    }

    /** Query that drops Person table. */
    public String dropPersonIfExist() {
        return "DROP TABLE IF EXISTS PERSON;";
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
        return "SELECT ID FROM PERSON WHERE SALARY BETWEEN ? AND ?";
    }


    /** Query that inserts new Person record. Has 4 parameters - fields of Person.*/
    public String insertIntoPerson() {
        return "INSERT INTO PERSON (id, first_name, last_name, salary) values (?, ?, ?, ?)";
    }
}
