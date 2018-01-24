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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.testframework.GridTestUtils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.concurrent.Callable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_PARSER_DISABLE_H2_FALLBACK;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Statement test.
 */
public class JdbcThinBulkLoadSelfTest extends JdbcThinAbstractDmlStatementSelfTest {
    /** SQL query. */
    private static final String SQL_PREPARED = "insert into Person(_key, id, firstName, lastName, age) values " +
        "(?, ?, ?, ?, ?)";

    /** Statement. */
    private Statement stmt;

    /** Prepared statement. */
    private PreparedStatement pstmt;

    private String BULKLOAD0_CSV_FILE = System.getProperty("IGNITE_HOME") +
        "/modules/clients/src/test/resources/bulkload0.csv";

    private CacheConfiguration cacheConfigWithIndexedTypes() {
        CacheConfiguration<?,?> cache = defaultCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setIndexedTypes(
            String.class, Person.class
        );

        return cache;
    }

    /**
     * @return Cache configuration for binary marshaller tests.
     */
    private CacheConfiguration cacheConfigWithQueryEntity() {
        CacheConfiguration<?,?> cache = defaultCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);

        QueryEntity e = new QueryEntity();

        e.setKeyType(String.class.getName());
        e.setValueType("Person");

        e.addQueryField("id", Integer.class.getName(), null);
        e.addQueryField("age", Integer.class.getName(), null);
        e.addQueryField("firstName", String.class.getName(), null);
        e.addQueryField("lastName", String.class.getName(), null);

        cache.setQueryEntities(Collections.singletonList(e));

        return cache;
    }

    private void createPersonTbl() throws SQLException {
        jdbcRun("create table " + QueryUtils.DFLT_SCHEMA + ".\"PersonTbl\" (\"id\" int primary key, \"age\" int, \"firstName\" varchar(30), \"lastName\" varchar(30))");
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stmt = conn.createStatement();

        pstmt = conn.prepareStatement(SQL_PREPARED);

        assertNotNull(stmt);
        assertFalse(stmt.isClosed());

        assertNotNull(pstmt);
        assertFalse(pstmt.isClosed());

        System.setProperty(IGNITE_SQL_PARSER_DISABLE_H2_FALLBACK, "TRUE");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        System.clearProperty(IGNITE_SQL_PARSER_DISABLE_H2_FALLBACK);

        if (stmt != null && !stmt.isClosed())
            stmt.close();

        if (pstmt != null && !pstmt.isClosed())
            pstmt.close();

        assertTrue(pstmt.isClosed());
        assertTrue(stmt.isClosed());

        super.afterTest();
    }

    /**
     * @throws SQLException If failed.
     *
     * FIXME SHQ: Tests to add
     * Inserting _key, _value of a wrong type
     * Read error in the middle of reading a file
     */
    public void testWrongFileName() {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeUpdate(
                    "copy from \"nonexistent\" into Person (_key, age, firstName, lastName) format csv");

                return null;
            }
        }, SQLException.class, "Failed to read file: 'nonexistent'");
    }

    public void testMissingTable() {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeUpdate(
                    "copy from \"" + BULKLOAD0_CSV_FILE + "\" into Peterson (_key, age, firstName, lastName) format csv");

                return null;
            }
        }, SQLException.class, "Table does not exist: PETERSON");
    }

    public void testWrongFormat() {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeUpdate(
                    "copy from \"" + BULKLOAD0_CSV_FILE + "\" into Person (_key, age, firstName, lastName) format lsd");

                return null;
            }
        }, SQLException.class, "Unknown format name: LSD");
    }

    public void testWrongColumn() {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeUpdate(
                    "copy from \"" + BULKLOAD0_CSV_FILE + "\" into Person (_key, age, firstName, lostName) format csv");

                return null;
            }
        }, SQLException.class, "Unexpected DML operation failure: Column \"LOSTNAME\" not found");
    }

    public void testSkippedColumns() {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeUpdate(
                    "copy from \"" + BULKLOAD0_CSV_FILE + "\" into Person format csv");

                return null;
            }
        }, SQLException.class, "Unexpected DML operation failure: Key is missing from query");
    }

    public void testWrongColumnType() throws SQLException {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeUpdate(
                    "copy from \"" + BULKLOAD0_CSV_FILE + "\" into Person (_key, firstName, age, lastName) format csv");

                return null;
            }
        }, SQLException.class, "Server error: Value conversion failed [from=java.lang.String, to=java.lang.Integer]");
    }

    /**
     * @throws SQLException If failed.
     */
    public void testFieldSubset() throws SQLException {
        int updatesCnt = stmt.executeUpdate(
            "copy from \"" + BULKLOAD0_CSV_FILE + "\" into Person (_key, age, firstName) format csv");

        assertEquals(2, updatesCnt);

        checkBulkload0CsvCacheContents(false);
    }

    /**
     * @throws SQLException If failed.
     */
    public void testCopyStmtWithoutColumnsToTable() throws SQLException {
        createPersonTbl();

        IgniteCache<Object, Object> personCache = grid(0).cache("PERSON");

        int updatesCnt = stmt.executeUpdate(
            "copy from \"" + BULKLOAD0_CSV_FILE + "\" into " + QueryUtils.DFLT_SCHEMA + ".\"PersonTbl\" format csv");

        assertEquals(2, updatesCnt);

        checkBulkload0CsvCacheContents(true);
    }

    /**
     * @throws SQLException If failed.
     */
    public void testDOA() throws SQLException {
        int updatesCnt = stmt.executeUpdate(
            "copy from \"" + BULKLOAD0_CSV_FILE + "\" into Person (_key, age, firstName, lastName) format csv");

        assertEquals(2, updatesCnt);

        checkBulkload0CsvCacheContents(true);
    }

    /**
     * @throws SQLException If failed.
     *
     */
    public void testBatchSize_1() throws SQLException {
        int updatesCnt = stmt.executeUpdate(
            "copy from \"" + BULKLOAD0_CSV_FILE + "\"" +
            " into Person (_key, age, firstName, lastName) format csv batch_size 1");

        assertEquals(2, updatesCnt);

        checkBulkload0CsvCacheContents(true);
    }

    private void checkBulkload0CsvCacheContents(boolean checkLastName) throws SQLException {
        ResultSet rs = stmt.executeQuery("select _key, age, firstName, lastName from Person");

        assert rs != null;

        int cnt = 0;

        while (rs.next()) {
            int id = rs.getInt("_key");

            if (id == 123) {
                assertEquals(12, rs.getInt("age"));
                assertEquals("FirstName123 MiddleName123", rs.getString("firstName"));
                if (checkLastName)
                    assertEquals("LastName123", rs.getString("lastName"));
            }
            else if (id == 456) {
                assertEquals(45, rs.getInt("age"));
                assertEquals("FirstName456", rs.getString("firstName"));
                if (checkLastName)
                    assertEquals("LastName456", rs.getString("lastName"));
            }
            else
                fail("Wrong ID: " + id);

            cnt++;
        }

        assertEquals(2, cnt);
    }
}
