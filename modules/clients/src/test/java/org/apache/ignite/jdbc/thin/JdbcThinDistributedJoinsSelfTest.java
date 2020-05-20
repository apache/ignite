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

package org.apache.ignite.jdbc.thin;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Tests for complex queries with distributed joins enabled (joins, etc.).
 */
public class JdbcThinDistributedJoinsSelfTest extends GridCommonAbstractTest {
    /** JDBC URL. */
    private static final String BASE_URL =  "jdbc:ignite:thin://127.0.0.1/default?distributedJoins=true";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGridsMultiThreaded(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<?,?> cache = defaultCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setSqlSchema("default");

        cfg.setCacheConfiguration(cache);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNonCollocatedDistributedJoinSingleCache() throws Exception {
        try(Statement stmt = DriverManager.getConnection(BASE_URL).createStatement()) {
            stmt.executeUpdate("CREATE TABLE person (id LONG, name VARCHAR(64), age LONG, city_id DOUBLE, PRIMARY KEY (name)) WITH \"backups=1\";");
            stmt.executeUpdate("CREATE TABLE medical_info (id LONG, name VARCHAR(64), age LONG, blood_group VARCHAR(64), PRIMARY KEY (id)) WITH \"backups=1\";");
            stmt.executeUpdate("CREATE TABLE blood_group_info_PJ (id LONG, blood_group VARCHAR(64), universal_donor VARCHAR(64), PRIMARY KEY (id)) WITH \"backups=1\";");
            stmt.executeUpdate("CREATE TABLE blood_group_info_P (id LONG, blood_group VARCHAR(64), universal_donor VARCHAR(64), PRIMARY KEY (blood_group)) WITH \"backups=1\";");

            stmt.executeUpdate("CREATE INDEX medical_info_name_ASC_IDX ON medical_info (name);");
            stmt.executeUpdate("CREATE INDEX medical_info_blood_group_ASC_IDX ON medical_info (blood_group);");

            stmt.executeUpdate("CREATE INDEX blood_group_info_PJ_blood_group_ASC_IDX ON blood_group_info_PJ (blood_group);");
        }

        populateData();

        checkQueries();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNonCollocatedDistributedJoin() throws Exception {
        try(Statement stmt = DriverManager.getConnection(BASE_URL).createStatement()) {
            stmt.executeUpdate("CREATE TABLE person (id LONG, name VARCHAR(64), age LONG, city_id DOUBLE, PRIMARY KEY (name)) " +
                "WITH \"cache_name=person,backups=1\";");
            stmt.executeUpdate("CREATE TABLE medical_info (id LONG, name VARCHAR(64), age LONG, blood_group VARCHAR(64), PRIMARY KEY (id)) " +
                "WITH \"cache_name=medical_info,backups=1\";");
            stmt.executeUpdate("CREATE TABLE blood_group_info_P (id LONG, blood_group VARCHAR(64), universal_donor VARCHAR(64), PRIMARY KEY (blood_group)) " +
                "WITH \"cache_name=blood_group_info_P,backups=1\";");
            stmt.executeUpdate("CREATE TABLE blood_group_info_PJ (id LONG, blood_group VARCHAR(64), universal_donor VARCHAR(64), PRIMARY KEY (id)) " +
                "WITH \"cache_name=blood_group_info_PJ,backups=1\";");


            stmt.executeUpdate("CREATE INDEX medical_info_name_ASC_IDX ON medical_info (name);");
            stmt.executeUpdate("CREATE INDEX medical_info_blood_group_ASC_IDX ON medical_info (blood_group);");

            stmt.executeUpdate("CREATE INDEX blood_group_info_PJ_blood_group_ASC_IDX ON blood_group_info_PJ (blood_group);");
            stmt.executeUpdate("CREATE INDEX blood_group_info_P_blood_group_ASC_IDX ON blood_group_info_P (blood_group);");
        }

        awaitPartitionMapExchange();
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

        try(Statement stmt = DriverManager.getConnection(BASE_URL).createStatement()) {
            final ResultSet resultSet = stmt.executeQuery("SELECT person.id, person.name, medical_info.blood_group, blood_group_info_PJ.universal_donor FROM person " +
                "LEFT JOIN medical_info ON medical_info.name = person.name " +
                "LEFT JOIN blood_group_info_PJ ON blood_group_info_PJ.blood_group = medical_info.blood_group;");

            res1 = queryResultAsString(resultSet);

            resultSet.close();
        }

        try(Statement stmt = DriverManager.getConnection(BASE_URL).createStatement()) {
            final ResultSet resultSet = stmt.executeQuery("SELECT person.id, person.name, medical_info.blood_group, blood_group_info_P.universal_donor FROM person " +
                "LEFT JOIN medical_info ON medical_info.name = person.name " +
                "LEFT JOIN blood_group_info_P ON blood_group_info_P.blood_group = medical_info.blood_group;");

            res2 = queryResultAsString(resultSet);

            resultSet.close();
        }

        log.info("Query1 result: \n" + res1);
        log.info("Query2 result: \n" + res2);

        String expOut = "2001,Shravya,null,null\n" +
            "2002,Kiran,O+,O+A+B+AB+\n" +
            "2003,Harika,AB+,AB+\n" +
            "2004,Srinivas,null,null\n" +
            "2005,Madhavi,A+,A+AB+\n" +
            "2006,Deeps,null,null\n" +
            "2007,Hope,null,null\n";

        assertEquals("Wrong result", expOut, res1);
        assertEquals("Wrong result", expOut, res2);
    }

    /**
     * Convert query result to string.
     *
     * @param res Query result set.
     * @return String representation.
     */
    private String queryResultAsString(ResultSet res) throws SQLException {
        List<String> results = new ArrayList<>();

        while (res.next()){
            StringBuilder sb = new StringBuilder('\t');

            sb.append(res.getLong(1)).append(',');
            sb.append(res.getString(2)).append(',');
            sb.append(res.getString(3)).append(',');
            sb.append(res.getString(4));

            results.add(sb.toString());
        }

        results.sort(String::compareTo);

        StringBuilder sb = new StringBuilder();

        for (String result : results)
            sb.append(result).append('\n');

        return sb.toString();
    }

    /** */
    private void populateData() throws SQLException {
        try(Statement stmt = DriverManager.getConnection(BASE_URL).createStatement()) {
            stmt.executeUpdate(" INSERT INTO person (id,name,age,city_id) VALUES " +
                "(2001,'Shravya',25,1.1), " +
                "(2002,'Kiran',26,1.1), " +
                "(2003,'Harika',26,2.4), " +
                "(2004,'Srinivas',24,3.2), " +
                "(2005,'Madhavi',23,3.2), " +
                "(2006,'Deeps',28,1.2), " +
                "(2007,'Hope',27,1.2);");

            stmt.executeUpdate("INSERT INTO medical_info (id,name,age,blood_group) VALUES " +
                "(2001,'Madhavi',23,'A+'), " +
                "(2002,'Diggi',27,'B+'), " +
                "(2003,'Kiran',26,'O+'), " +
                "(2004,'Harika',26,'AB+');");

            stmt.executeUpdate("INSERT INTO blood_group_info_PJ (id,blood_group,universal_donor) VALUES " +
                "(2001,'A+','A+AB+'), " +
                "(2002,'O+','O+A+B+AB+'), " +
                "(2003,'B+','B+AB+'), " +
                "(2004,'AB+','AB+'), " +
                "(2005,'O-','EveryOne');");

            stmt.executeUpdate("INSERT INTO blood_group_info_P (id,blood_group,universal_donor) VALUES " +
                "(2001,'A+','A+AB+'), " +
                "(2002,'O+','O+A+B+AB+'), " +
                "(2003,'B+','B+AB+'), " +
                "(2004,'AB+','AB+'), " +
                "(2005,'O-','EveryOne');");
        }
    }
}
