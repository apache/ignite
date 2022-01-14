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

package org.apache.ignite.internal.runner.app.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests for complex queries with joins.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-15655")
public class ItJdbcJoinsSelfTest extends AbstractJdbcSelfTest {
    /**
     * Check distributed OUTER join of 3 tables (T1 -> T2 -> T3) returns correct result for non-collocated data.
     *
     * <ul>
     *     <li>Create tables.</li>
     *     <li>Put data into tables.</li>
     *     <li>Check query with distributedJoin=true returns correct results.</li>
     * </ul>
     *
     * @throws Exception If failed.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-16219")
    public void testJoin() throws Exception {
        stmt.executeUpdate("CREATE TABLE PUBLIC.PERSON"
                + " (ID INT, NAME VARCHAR(64), AGE INT, CITY_ID DOUBLE, PRIMARY KEY (NAME));");
        stmt.executeUpdate("CREATE TABLE PUBLIC.MEDICAL_INFO"
                + " (ID INT, NAME VARCHAR(64), AGE INT, BLOOD_GROUP VARCHAR(64), PRIMARY KEY (ID));");
        stmt.executeUpdate("CREATE TABLE PUBLIC.BLOOD_GROUP_INFO_PJ"
                + " (ID INT, BLOOD_GROUP VARCHAR(64), UNIVERSAL_DONOR VARCHAR(64), PRIMARY KEY (ID));");
        stmt.executeUpdate("CREATE TABLE PUBLIC.BLOOD_GROUP_INFO_P"
                + " (ID INT, BLOOD_GROUP VARCHAR(64), UNIVERSAL_DONOR VARCHAR(64), PRIMARY KEY (BLOOD_GROUP));");

        populateData();

        checkQueries();
    }

    /**
     * Start queries and check query results.
     *
     */
    private void checkQueries() throws SQLException {
        String res1;
        String res2;

        // Join on non-primary key.
        try (final ResultSet resultSet = stmt.executeQuery("SELECT person.id, person.name,"
                    + " medical_info.blood_group, blood_group_info_PJ.universal_donor FROM person "
                    + "LEFT JOIN medical_info ON medical_info.name = person.name "
                    + "LEFT JOIN blood_group_info_PJ ON blood_group_info_PJ.blood_group "
                    + "= medical_info.blood_group;")) {

            res1 = queryResultAsString(resultSet);
        }

        // Join on primary key.
        try (final ResultSet resultSet = stmt.executeQuery("SELECT person.id, person.name,"
                + " medical_info.blood_group, blood_group_info_P.universal_donor FROM person "
                + "LEFT JOIN medical_info ON medical_info.name = person.name "
                + "LEFT JOIN blood_group_info_P ON blood_group_info_P.blood_group "
                + "= medical_info.blood_group;")) {

            res2 = queryResultAsString(resultSet);
        }

        log.info("Query1 result: \n" + res1);
        log.info("Query2 result: \n" + res2);

        String expOut = "2001,Shravya,null,null\n"
                + "2002,Kiran,O+,O+A+B+AB+\n"
                + "2003,Harika,AB+,AB+\n"
                + "2004,Srinivas,null,null\n"
                + "2005,Madhavi,A+,A+AB+\n"
                + "2006,Deeps,null,null\n"
                + "2007,Hope,null,null\n";

        assertEquals(expOut, res1, "Wrong result");
        assertEquals(expOut, res2, "Wrong result");
    }

    /**
     * Convert query result to string.
     *
     * @param res Query result set.
     * @return String representation.
     */
    private String queryResultAsString(ResultSet res) throws SQLException {
        List<String> results = new ArrayList<>();

        while (res.next()) {
            String row = String.valueOf(res.getLong(1))
                    + ',' + res.getString(2)
                    + ',' + res.getString(3)
                    + ',' + res.getString(4);

            results.add(row);
        }

        results.sort(String::compareTo);

        StringBuilder sb = new StringBuilder();

        for (String result : results) {
            sb.append(result).append('\n');
        }

        return sb.toString();
    }

    private void populateData() throws SQLException {
        stmt.executeUpdate(" INSERT INTO PUBLIC.PERSON (ID,NAME,AGE,CITY_ID) VALUES "
                + "(2001,'Shravya',25,1.1), "
                + "(2002,'Kiran',26,1.1), "
                + "(2003,'Harika',26,2.4), "
                + "(2004,'Srinivas',24,3.2), "
                + "(2005,'Madhavi',23,3.2), "
                + "(2006,'Deeps',28,1.2), "
                + "(2007,'Hope',27,1.2);");

        stmt.executeUpdate("INSERT INTO PUBLIC.MEDICAL_INFO (id,name,age,blood_group) VALUES "
                + "(2001,'Madhavi',23,'A+'), "
                + "(2002,'Diggi',27,'B+'), "
                + "(2003,'Kiran',26,'O+'), "
                + "(2004,'Harika',26,'AB+');");

        stmt.executeUpdate("INSERT INTO PUBLIC.BLOOD_GROUP_INFO_PJ (id,blood_group,universal_donor) VALUES "
                + "(2001,'A+','A+AB+'), "
                + "(2002,'O+','O+A+B+AB+'), "
                + "(2003,'B+','B+AB+'), "
                + "(2004,'AB+','AB+'), "
                + "(2005,'O-','EveryOne');");

        stmt.executeUpdate("INSERT INTO PUBLIC.BLOOD_GROUP_INFO_P (id,blood_group,universal_donor) VALUES "
                + "(2001,'A+','A+AB+'), "
                + "(2002,'O+','O+A+B+AB+'), "
                + "(2003,'B+','B+AB+'), "
                + "(2004,'AB+','AB+'), "
                + "(2005,'O-','EveryOne');");
    }
}
