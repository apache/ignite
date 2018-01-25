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

import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.testframework.GridTestUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_PARSER_DISABLE_H2_FALLBACK;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * COPY statement tests.
 */
public class JdbcThinBulkLoadSelfTest extends JdbcThinAbstractDmlStatementSelfTest {
    public static final String TBL_NAME = "Person";

    /** JDBC statement. */
    private Statement stmt;

    /** A CSV file with zero records */
    private String BULKLOAD_EMPTY_CSV_FILE = System.getProperty("IGNITE_HOME") +
        "/modules/clients/src/test/resources/bulkload0.csv";

    /** A CSV file with one record. */
    private String BULKLOAD_ONE_LINE_CSV_FILE = System.getProperty("IGNITE_HOME") +
        "/modules/clients/src/test/resources/bulkload1.csv";

    /** A CSV file with two records. */
    private String BULKLOAD0_CSV_FILE = System.getProperty("IGNITE_HOME") +
        "/modules/clients/src/test/resources/bulkload2.csv";

    /** A file with UTF records. */
    private String BULKLOAD_UTF_CSV_FILE = System.getProperty("IGNITE_HOME") +
        "/modules/clients/src/test/resources/bulkload2_utf.csv";

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfig() {
        return cacheConfigWithIndexedTypes();
    }

    /**
     * Creates cache configuration with {@link QueryEntity} created
     * using {@link CacheConfiguration#setIndexedTypes(Class[])} call.
     *
     * @return The cache configuration.
     */
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
     * Creates cache configuration with {@link QueryEntity} created
     * using {@link CacheConfiguration#setQueryEntities(Collection)} call.
     *
     * @return The cache configuration.
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

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        System.setProperty(IGNITE_SQL_PARSER_DISABLE_H2_FALLBACK, "TRUE");

        stmt = conn.createStatement();

        assertNotNull(stmt);
        assertFalse(stmt.isClosed());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (stmt != null && !stmt.isClosed())
            stmt.close();

        assertTrue(stmt.isClosed());

        System.clearProperty(IGNITE_SQL_PARSER_DISABLE_H2_FALLBACK);

        super.afterTest();
    }

    /**
     * Dead-on-arrival test. Imports two-entry CSV file into a table and checks
     * the created entries using SELECT statement.
     *
     * @throws SQLException If failed.
     */
    public void testDOA() throws SQLException {
        int updatesCnt = stmt.executeUpdate(
            "copy from \"" + BULKLOAD0_CSV_FILE + "\" into " + TBL_NAME +
                " (_key, age, firstName, lastName)" +
                " format csv");

        assertEquals(2, updatesCnt);

        checkCacheContents(TBL_NAME, true, 2);
    }

    /**
     * Imports two-entry CSV file with UTF-8 characters into a table and checks
     * the created entries using SELECT statement.
     *
     * @throws SQLException If failed.
     */
    public void testUtf() throws SQLException {
        int updatesCnt = stmt.executeUpdate(
            "copy from \"" + BULKLOAD_UTF_CSV_FILE + "\" into " + TBL_NAME +
                " (_key, age, firstName, lastName)" +
                " format csv");

        assertEquals(2, updatesCnt);

        checkUtfCacheContents(TBL_NAME, true, 2);
    }

    /**
     * Imports two-entry CSV file with UTF-8 characters into a table using batch size of one byte
     * (thus splitting each two-byte UTF-8 character into two batches)
     * and checks the created entries using SELECT statement.
     *
     * @throws SQLException If failed.
     */
    public void testUtfBatchSize_1() throws SQLException {
        int updatesCnt = stmt.executeUpdate(
            "copy from \"" + BULKLOAD_UTF_CSV_FILE + "\" into " + TBL_NAME +
                " (_key, age, firstName, lastName)" +
                " format csv batch_size 1");

        assertEquals(2, updatesCnt);

        checkUtfCacheContents(TBL_NAME, true, 2);
    }

    /**
     * Imports one-entry CSV file into a table and checks the entry created using SELECT statement.
     *
     * @throws SQLException If failed.
     */
    public void testOneLineFile() throws SQLException {
        int updatesCnt = stmt.executeUpdate(
            "copy from \"" + BULKLOAD_ONE_LINE_CSV_FILE + "\" into " + TBL_NAME +
                " (_key, age, firstName, lastName)" +
                " format csv");

        assertEquals(1, updatesCnt);

        checkCacheContents(TBL_NAME, true, 1);
    }

    /**
     * Imports zero-entry CSV file into a table and checks that no entries are created
     * using SELECT statement.
     *
     * @throws SQLException If failed.
     */
    public void testEmptyFile() throws SQLException {
        int updatesCnt = stmt.executeUpdate(
            "copy from \"" + BULKLOAD_ONE_LINE_CSV_FILE + "\" into " + TBL_NAME +
                " (_key, age, firstName, lastName)" +
                " format csv");

        assertEquals(0, updatesCnt);

        checkCacheContents(TBL_NAME, true, 0);
    }

    /**
     * Checks invalid 'FROM' keyword in the SQL command.
     *
     * @throws SQLException If failed.
     */
    public void testWrongFromSyntax() {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeUpdate(
                    "copy grom \"nonexistent\" into Person" +
                        " (_key, age, firstName, lastName)" +
                        " format csv");

                return null;
            }
        }, SQLException.class, "Unexpected token: \"GROM\" (expected: \"FROM\")");
    }

    /**
     * Checks that error is reported for a non-existent file.
     *
     * @throws SQLException If failed.
     */
    public void testWrongFileName() {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeUpdate(
                    "copy from \"nonexistent\" into Person" +
                        " (_key, age, firstName, lastName)" +
                        " format csv");

                return null;
            }
        }, SQLException.class, "Failed to read file: 'nonexistent'");
    }

    /**
     * Checks that error is reported for wrong INTO keyword.
     *
     * @throws SQLException If failed.
     */
    public void testWrongIntoSyntax() {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeUpdate(
                    "copy from \"nonexistent\" to Person" +
                        " (_key, age, firstName, lastName)" +
                        " format csv");

                return null;
            }
        }, SQLException.class, "Unexpected token: \"TO\" (expected: \"INTO\")");
    }

    /**
     * Checks that error is reported if the destination table is missing.
     *
     * @throws SQLException If failed.
     */
    public void testMissingTable() {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeUpdate(
                    "copy from \"" + BULKLOAD0_CSV_FILE + "\" into Peterson" +
                        " (_key, age, firstName, lastName)" +
                        " format csv");

                return null;
            }
        }, SQLException.class, "Table does not exist: PETERSON");
    }

    /**
     * Checks that error is reported if empty list of columns is specified in the SQL command.
     *
     * @throws SQLException If failed.
     */
    public void testEmptyColumnList() {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeUpdate(
                    "copy from \"" + BULKLOAD0_CSV_FILE + "\" into Person" +
                        " ()" +
                        " format csv");

                return null;
            }
        }, SQLException.class, "Unexpected token: \")\" (expected: \"[identifier]\")");
    }

    /**
     * Checks that error is reported when a non-existing column is specified in the SQL command.
     *
     * @throws SQLException If failed.
     */
    public void testWrongColumnName() {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeUpdate(
                    "copy from \"" + BULKLOAD0_CSV_FILE + "\" into Person" +
                        " (_key, age, firstName, lostName)" +
                        " format csv");

                return null;
            }
        }, SQLException.class, "Column \"LOSTNAME\" not found");
    }

    /**
     * Checks that error is reported when list of columns is not specified in the SQL command.
     *
     * @throws SQLException If failed.
     */
    public void testSkippedColumns() {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeUpdate(
                    "copy from \"" + BULKLOAD0_CSV_FILE + "\" into Person" +
                        " format csv");

                return null;
            }
        }, SQLException.class, "Unexpected token: \"FORMAT\" (expected: \"(\")");
    }

    /**
     * Checks that error is reported if field read from CSV file cannot be converted to the type of the column.
     *
     * @throws SQLException If failed.
     */
    public void testWrongColumnType() throws SQLException {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeUpdate(
                    "copy from \"" + BULKLOAD0_CSV_FILE + "\" into Person" +
                        " (_key, firstName, age, lastName)" +
                        " format csv");

                return null;
            }
        }, SQLException.class, "Value conversion failed [from=java.lang.String, to=java.lang.Integer]");
    }

    /**
     * Checks that if even a subset of fields is imported, the imported fields are set correctly.
     *
     * @throws SQLException If failed.
     */
    public void testFieldsSubset() throws SQLException {
        int updatesCnt = stmt.executeUpdate(
            "copy from \"" + BULKLOAD0_CSV_FILE + "\" into " + TBL_NAME +
                " (_key, age, firstName)" +
                " format csv");

        assertEquals(2, updatesCnt);

        checkCacheContents(TBL_NAME, false, 2);
    }

    /**
     * Checks that error is reported if the format clause is missing in the SQL command.
     *
     * @throws SQLException If failed.
     */
    public void testMissingFormat() {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeUpdate(
                    "copy from \"" + BULKLOAD0_CSV_FILE + "\" into Person" +
                        " (_key, age, firstName, lastName)");

                return null;
            }
        }, SQLException.class, "Unexpected end of command (expected: \"FORMAT\")");
    }

    /**
     * Checks that error is reported if a not supported format name is specified in the SQL command.
     *
     * @throws SQLException If failed.
     */
    public void testWrongFormat() {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeUpdate(
                    "copy from \"" + BULKLOAD0_CSV_FILE + "\" into Person" +
                        " (_key, age, firstName, lastName)" +
                        " format lsd");

                return null;
            }
        }, SQLException.class, "Unknown format name: LSD");
    }

    /**
     * Checks that bulk load works when we create table using 'CREATE TABLE' command
     * (the other tests use {@link CacheConfiguration#setIndexedTypes(Class[])} to create a
     * table).
     *
     * @throws SQLException If failed.
     */
    public void testCreateAndBulkLoadTable() throws SQLException {
        String tblName = QueryUtils.DFLT_SCHEMA + ".\"PersonTbl\"";

        jdbcRun("create table " + tblName +
            " (id int primary key, age int, firstName varchar(30), lastName varchar(30))");

        int updatesCnt = stmt.executeUpdate(
            "copy from \"" + BULKLOAD0_CSV_FILE + "\" into " + tblName +
                "(_key, age, firstName, lastName)" +
                " format csv");

        assertEquals(2, updatesCnt);

        checkCacheContents(tblName, true, 2);
    }

    /**
     * Checks that bulk load works when we create table with {@link CacheConfiguration#setQueryEntities(Collection)}
     * (the other tests use {@link CacheConfiguration#setIndexedTypes(Class[])} to create a
     * table).
     *
     * @throws SQLException If failed.
     */
    public void testConfigureQueryEntityAndBulkLoad() throws SQLException {
        ignite(0).getOrCreateCache(cacheConfigWithQueryEntity());

        int updatesCnt = stmt.executeUpdate(
            "copy from \"" + BULKLOAD0_CSV_FILE + "\" into " + TBL_NAME +
                " (_key, age, firstName, lastName)" +
                " format csv");

        assertEquals(2, updatesCnt);

        checkCacheContents(TBL_NAME, true, 2);
    }

    /**
     * Checks that bulk load works when we use batch size of 1 byte and thus
     * create multiple batches per COPY.
     *
     * @throws SQLException If failed.
     */
    public void testBatchSize_1() throws SQLException {
        int updatesCnt = stmt.executeUpdate(
            "copy from \"" + BULKLOAD0_CSV_FILE + "\"" +
            " into " + TBL_NAME + " (_key, age, firstName, lastName) format csv batch_size 1");

        assertEquals(2, updatesCnt);

        checkCacheContents(TBL_NAME, true, 2);
    }

    /**
     * Checks cache contents for a typical test using SQL SELECT command.
     *
     * @param tblName Table name to query.
     * @param checkLastName Check 'lastName' column (not imported in some tests).
     * @param recCnt Number of records to expect.
     * @throws SQLException When one of checks has failed.
     */
    private void checkCacheContents(String tblName, boolean checkLastName, int recCnt) throws SQLException {
        ResultSet rs = stmt.executeQuery("select _key, age, firstName, lastName from " + tblName);

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

        assertEquals(recCnt, cnt);
    }

    /**
     * Checks cache contents for a UTF-8 bulk load tests using SQL SELECT command.
     *
     * @param tblName Table name to query.
     * @param checkLastName Check 'lastName' column (not imported in some tests).
     * @param recCnt Number of records to expect.
     * @throws SQLException When one of checks has failed.
     */
    private void checkUtfCacheContents(String tblName, boolean checkLastName, int recCnt) throws SQLException {
        ResultSet rs = stmt.executeQuery("select _key, age, firstName, lastName from " + tblName);

        assert rs != null;

        int cnt = 0;

        while (rs.next()) {
            int id = rs.getInt("_key");

            if (id == 123) {
                assertEquals(12, rs.getInt("age"));
                assertEquals("Имя123 Отчество123", rs.getString("firstName"));
                if (checkLastName)
                    assertEquals("Фамилия123", rs.getString("lastName"));
            }
            else if (id == 456) {
                assertEquals(45, rs.getInt("age"));
                assertEquals("Имя456", rs.getString("firstName"));
                if (checkLastName)
                    assertEquals("Фамилия456", rs.getString("lastName"));
            }
            else
                fail("Wrong ID: " + id);

            cnt++;
        }

        assertEquals(recCnt, cnt);
    }
}
