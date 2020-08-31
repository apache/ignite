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

package org.apache.ignite.internal.jdbc2;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteJdbcDriver.CFG_URL_PREFIX;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Statement test.
 */
public class JdbcStatementSelfTest extends GridCommonAbstractTest {
    /** JDBC URL. */
    private static final String BASE_URL = CFG_URL_PREFIX
        + "cache=default:multipleStatementsAllowed=true@modules/clients/src/test/config/jdbc-config.xml";

    /** SQL query. */
    private static final String SQL = "select * from Person where age > 30";

    /** Connection. */
    private Connection conn;

    /** Statement. */
    private Statement stmt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<?,?> cache = defaultCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setIndexedTypes(
            String.class, Person.class
        );

        cfg.setCacheConfiguration(cache);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(3);

        IgniteCache<String, Person> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        assert cache != null;

        cache.put("p1", new Person(1, "John", "White", 25));
        cache.put("p2", new Person(2, "Joe", "Black", 35));
        cache.put("p3", new Person(3, "Mike", "Green", 40));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        conn = DriverManager.getConnection(BASE_URL);
        stmt = conn.createStatement();

        assertNotNull(stmt);
        assertFalse(stmt.isClosed());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (stmt != null && !stmt.isClosed())
            stmt.close();

        conn.close();

        assertTrue(stmt.isClosed());
        assertTrue(conn.isClosed());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExecuteQuery() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        assert rs != null;

        int cnt = 0;

        while (rs.next()) {
            int id = rs.getInt("id");

            if (id == 2) {
                assert "Joe".equals(rs.getString("firstName"));
                assert "Black".equals(rs.getString("lastName"));
                assert rs.getInt("age") == 35;
            }
            else if (id == 3) {
                assert "Mike".equals(rs.getString("firstName"));
                assert "Green".equals(rs.getString("lastName"));
                assert rs.getInt("age") == 40;
            }
            else
                assert false : "Wrong ID: " + id;

            cnt++;
        }

        assert cnt == 2;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExecute() throws Exception {
        assert stmt.execute(SQL);

        ResultSet rs = stmt.getResultSet();

        assert rs != null;

        int cnt = 0;

        while (rs.next()) {
            int id = rs.getInt("id");

            if (id == 2) {
                assert "Joe".equals(rs.getString("firstName"));
                assert "Black".equals(rs.getString("lastName"));
                assert rs.getInt("age") == 35;
            }
            else if (id == 3) {
                assert "Mike".equals(rs.getString("firstName"));
                assert "Green".equals(rs.getString("lastName"));
                assert rs.getInt("age") == 40;
            }
            else
                assert false : "Wrong ID: " + id;

            cnt++;
        }

        assert cnt == 2;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMaxRows() throws Exception {
        stmt.setMaxRows(1);

        ResultSet rs = stmt.executeQuery(SQL);

        assert rs != null;

        int cnt = 0;

        while (rs.next()) {
            int id = rs.getInt("id");

            if (id == 2) {
                assert "Joe".equals(rs.getString("firstName"));
                assert "Black".equals(rs.getString("lastName"));
                assert rs.getInt("age") == 35;
            }
            else if (id == 3) {
                assert "Mike".equals(rs.getString("firstName"));
                assert "Green".equals(rs.getString("lastName"));
                assert rs.getInt("age") == 40;
            }
            else
                assert false : "Wrong ID: " + id;

            cnt++;
        }

        assert cnt == 1;

        stmt.setMaxRows(0);

        rs = stmt.executeQuery(SQL);

        assert rs != null;

        cnt = 0;

        while (rs.next()) {
            int id = rs.getInt("id");

            if (id == 2) {
                assert "Joe".equals(rs.getString("firstName"));
                assert "Black".equals(rs.getString("lastName"));
                assert rs.getInt("age") == 35;
            }
            else if (id == 3) {
                assert "Mike".equals(rs.getString("firstName"));
                assert "Green".equals(rs.getString("lastName"));
                assert rs.getInt("age") == 40;
            }
            else
                assert false : "Wrong ID: " + id;

            cnt++;
        }

        assert cnt == 2;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExecuteQueryMultipleOnlyResultSets() throws Exception {
        assert conn.getMetaData().supportsMultipleResultSets();

        int stmtCnt = 10;

        StringBuilder sql = new StringBuilder();

        for (int i = 0; i < stmtCnt; ++i)
            sql.append("select ").append(i).append("; ");

        assert stmt.execute(sql.toString());

        for (int i = 0; i < stmtCnt; ++i) {
            assert stmt.getMoreResults();

            ResultSet rs = stmt.getResultSet();

            assert rs.next();
            assert rs.getInt(1) == i;
            assert !rs.next();
        }

        assert !stmt.getMoreResults();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExecuteQueryMultipleOnlyDml() throws Exception {
        assert conn.getMetaData().supportsMultipleResultSets();

        conn.setSchema(null);

        int stmtCnt = 10;

        StringBuilder sql = new StringBuilder(
            "drop table if exists test; create table test(ID int primary key, NAME varchar(20)); ");

        for (int i = 0; i < stmtCnt; ++i)
            sql.append("insert into test (ID, NAME) values (" + i + ", 'name_" + i + "'); ");

        assert !stmt.execute(sql.toString());

        // DROP TABLE statement
        assert stmt.getResultSet() == null;
        assert stmt.getUpdateCount() == 0;

        // CREATE TABLE statement
        assert stmt.getResultSet() == null;
        assert stmt.getUpdateCount() == 0;

        for (int i = 0; i < stmtCnt; ++i) {
            assert stmt.getMoreResults();

            assert stmt.getResultSet() == null;
            assert stmt.getUpdateCount() == 1;
        }

        assert !stmt.getMoreResults();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExecuteQueryMultipleMixed() throws Exception {
        assert conn.getMetaData().supportsMultipleResultSets();

        conn.setSchema(null);

        int stmtCnt = 10;

        StringBuilder sql = new StringBuilder(
            "drop table if exists test; create table test(ID int primary key, NAME varchar(20)); ");

        for (int i = 0; i < stmtCnt; ++i) {
            if (i % 2 == 0)
                sql.append(" insert into test (ID, NAME) values (" + i + ", 'name_" + i + "'); ");
            else
                sql.append(" select * from test where id < " + i + "; ");
        }

        assert !stmt.execute(sql.toString());

        // DROP TABLE statement
        assert stmt.getResultSet() == null;
        assert stmt.getUpdateCount() == 0;

        // CREATE TABLE statement
        assert stmt.getResultSet() == null;
        assert stmt.getUpdateCount() == 0;

        boolean notEmptyResult = false;

        for (int i = 0; i < stmtCnt; ++i) {
            assert stmt.getMoreResults();

            if (i % 2 == 0) {
                assert stmt.getResultSet() == null;
                assert stmt.getUpdateCount() == 1;
            }
            else {
                assert stmt.getUpdateCount() == -1;

                ResultSet rs = stmt.getResultSet();

                assert rs.getMetaData().getColumnCount() == 2;

                int rowsCnt = 0;

                while (rs.next())
                    rowsCnt++;

                assert rowsCnt <= (i + 1) / 2;

                if (rowsCnt == (i + 1) / 2)
                    notEmptyResult = true;
            }
        }

        assert notEmptyResult;

        assert !stmt.getMoreResults();
    }

    /**
     * Person.
     */
    private static class Person implements Serializable {
        /** ID. */
        @QuerySqlField
        private final int id;

        /** First name. */
        @QuerySqlField(index = false)
        private final String firstName;

        /** Last name. */
        @QuerySqlField(index = false)
        private final String lastName;

        /** Age. */
        @QuerySqlField
        private final int age;

        /**
         * @param id ID.
         * @param firstName First name.
         * @param lastName Last name.
         * @param age Age.
         */
        private Person(int id, String firstName, String lastName, int age) {
            assert !F.isEmpty(firstName);
            assert !F.isEmpty(lastName);
            assert age > 0;

            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
            this.age = age;
        }
    }
}
