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

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.testframework.GridTestUtils;

import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.Callable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_PARSER_DISABLE_H2_FALLBACK;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.util.IgniteUtils.resolveIgnitePath;

/**
 * COPY statement tests.
 */
public abstract class JdbcThinBulkLoadAbstractSelfTest extends JdbcThinAbstractDmlStatementSelfTest {
    /** Subdirectory with CSV files */
    private static final String CSV_FILE_SUBDIR = "/modules/clients/src/test/resources/";

    /** A CSV file with zero records */
    private static final String BULKLOAD_EMPTY_CSV_FILE =
        Objects.requireNonNull(resolveIgnitePath(CSV_FILE_SUBDIR + "bulkload0.csv")).getAbsolutePath();

    /** A CSV file with one record. */
    private static final String BULKLOAD_ONE_LINE_CSV_FILE =
        Objects.requireNonNull(resolveIgnitePath(CSV_FILE_SUBDIR + "bulkload1.csv")).getAbsolutePath();

    /** A CSV file with two records. */
    private static final String BULKLOAD_TWO_LINES_CSV_FILE =
        Objects.requireNonNull(resolveIgnitePath(CSV_FILE_SUBDIR + "bulkload2.csv")).getAbsolutePath();

    /** A CSV file in UTF-8. */
    private static final String BULKLOAD_UTF_CSV_FILE =
        Objects.requireNonNull(resolveIgnitePath(CSV_FILE_SUBDIR + "bulkload2_utf.csv")).getAbsolutePath();

    /** Default table name. */
    private static final String TBL_NAME = "Person";

    /** Basic COPY statement used in majority of the tests. */
    public static final String BASIC_SQL_COPY_STMT =
        "copy from \"" + BULKLOAD_TWO_LINES_CSV_FILE + "\"" +
        " into " + TBL_NAME +
        " (_key, age, firstName, lastName)" +
        " format csv";

    /** JDBC statement. */
    private Statement stmt;

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
    @SuppressWarnings("unchecked")
    private CacheConfiguration cacheConfigWithIndexedTypes() {
        CacheConfiguration<?,?> cache = defaultCacheConfiguration();

        cache.setCacheMode(cacheMode());
        cache.setAtomicityMode(atomicityMode());
        cache.setWriteSynchronizationMode(FULL_SYNC);

        if (cacheMode() == PARTITIONED)
            cache.setBackups(1);

        if (nearCache())
            cache.setNearConfiguration(new NearCacheConfiguration());

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

    /**
     * Returns true if we are testing near cache.
     *
     * @return true if we are testing near cache.
     */
    protected abstract boolean nearCache();

    /**
     * Returns cache atomicity mode we are testing.
     *
     * @return The cache atomicity mode we are testing.
     */
    protected abstract CacheAtomicityMode atomicityMode();

    /**
     * Returns cache mode we are testing.
     *
     * @return The cache mode we are testing.
     */
    protected abstract CacheMode cacheMode();

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
    public void testBasicStatement() throws SQLException {
        int updatesCnt = stmt.executeUpdate(BASIC_SQL_COPY_STMT);

        assertEquals(2, updatesCnt);

        checkCacheContents(TBL_NAME, true, 2);
    }

    /**
     * Imports zero-entry CSV file into a table and checks that no entries are created
     * using SELECT statement.
     *
     * @throws SQLException If failed.
     */
    public void testEmptyFile() throws SQLException {
        int updatesCnt = stmt.executeUpdate(
            "copy from \"" + BULKLOAD_EMPTY_CSV_FILE + "\" into " + TBL_NAME +
            " (_key, age, firstName, lastName)" +
            " format csv");

        assertEquals(0, updatesCnt);

        checkCacheContents(TBL_NAME, true, 0);
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
     * Checks that bulk load works when we use batch size of 1 byte and thus
     * create multiple batches per COPY.
     *
     * @throws SQLException If failed.
     */
    public void testPacketSize_1() throws SQLException {
        int updatesCnt = stmt.executeUpdate(BASIC_SQL_COPY_STMT + " packet_size 1");

        assertEquals(2, updatesCnt);

        checkCacheContents(TBL_NAME, true, 2);
    }

    /**
     * Imports two-entry CSV file with UTF-8 characters into a table using packet size of one byte
     * (thus splitting each two-byte UTF-8 character into two batches)
     * and checks the created entries using SELECT statement.
     *
     * @throws SQLException If failed.
     */
    public void testUtfPacketSize_1() throws SQLException {
        int updatesCnt = stmt.executeUpdate(
            "copy from \"" + BULKLOAD_UTF_CSV_FILE + "\" into " + TBL_NAME +
            " (_key, age, firstName, lastName)" +
            " format csv packet_size 1");

        assertEquals(2, updatesCnt);

        checkUtfCacheContents(TBL_NAME, true, 2);
    }

    /**
     * Checks that error is reported for a non-existent file.
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
     * Checks that error is reported if the destination table is missing.
     */
    public void testMissingTable() {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeUpdate(
                    "copy from \"" + BULKLOAD_TWO_LINES_CSV_FILE + "\" into Peterson" +
                    " (_key, age, firstName, lastName)" +
                    " format csv");

                return null;
            }
        }, SQLException.class, "Table does not exist: PETERSON");
    }

    /**
     * Checks that error is reported when a non-existing column is specified in the SQL command.
     */
    public void testWrongColumnName() {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeUpdate(
                    "copy from \"" + BULKLOAD_TWO_LINES_CSV_FILE + "\" into Person" +
                    " (_key, age, firstName, lostName)" +
                    " format csv");

                return null;
            }
        }, SQLException.class, "Column \"LOSTNAME\" not found");
    }

    /**
     * Checks that error is reported if field read from CSV file cannot be converted to the type of the column.
     */
    public void testWrongColumnType() {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeUpdate(
                    "copy from \"" + BULKLOAD_TWO_LINES_CSV_FILE + "\" into Person" +
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
            "copy from \"" + BULKLOAD_TWO_LINES_CSV_FILE + "\" into " + TBL_NAME +
            " (_key, age, firstName)" +
            " format csv");

        assertEquals(2, updatesCnt);

        checkCacheContents(TBL_NAME, false, 2);
    }

    /**
     * Checks that bulk load works when we create table using 'CREATE TABLE' command.
     * <p>
     * The majority of the tests in this class use {@link CacheConfiguration#setIndexedTypes(Class[])}
     * to create the table.
     *
     * @throws SQLException If failed.
     */
    public void testCreateAndBulkLoadTable() throws SQLException {
        String tblName = QueryUtils.DFLT_SCHEMA + ".\"PersonTbl\"";

        execute(conn, "create table " + tblName +
            " (id int primary key, age int, firstName varchar(30), lastName varchar(30))");

        int updatesCnt = stmt.executeUpdate(
            "copy from \"" + BULKLOAD_TWO_LINES_CSV_FILE + "\" into " + tblName +
            "(_key, age, firstName, lastName)" +
            " format csv");

        assertEquals(2, updatesCnt);

        checkCacheContents(tblName, true, 2);
    }

    /**
     * Checks that bulk load works when we create table with {@link CacheConfiguration#setQueryEntities(Collection)}.
     * <p>
     * The majority of the tests in this class use {@link CacheConfiguration#setIndexedTypes(Class[])}
     * to create a table.
     *
     * @throws SQLException If failed.
     */
    @SuppressWarnings("unchecked")
    public void testConfigureQueryEntityAndBulkLoad() throws SQLException {
        ignite(0).getOrCreateCache(cacheConfigWithQueryEntity());

        int updatesCnt = stmt.executeUpdate(BASIC_SQL_COPY_STMT);

        assertEquals(2, updatesCnt);

        checkCacheContents(TBL_NAME, true, 2);
    }

    /**
     * Verifies exception thrown if COPY is added into a batch.
     */
    public void testMultipleStatement() {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.addBatch(BASIC_SQL_COPY_STMT);

                stmt.addBatch("copy from \"" + BULKLOAD_ONE_LINE_CSV_FILE + "\" into " + TBL_NAME +
                    " (_key, age, firstName, lastName)" +
                    " format csv");

                stmt.addBatch("copy from \"" + BULKLOAD_UTF_CSV_FILE + "\" into " + TBL_NAME +
                    " (_key, age, firstName, lastName)" +
                    " format csv");

                stmt.executeBatch();

                return null;
            }
        }, BatchUpdateException.class, "COPY command cannot be executed in batch mode.");
    }

    /**
     * Verifies that COPY command is rejected by Statement.executeQuery().
     */
    public void testExecuteQuery() {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeQuery(BASIC_SQL_COPY_STMT);

                return null;
            }
        }, SQLException.class, "The query isn't SELECT query");
    }

    /**
     * Verifies that COPY command works in Statement.execute().
     *
     * @throws SQLException If failed.
     */
    public void testExecute() throws SQLException {
        boolean isRowSet = stmt.execute(BASIC_SQL_COPY_STMT);

        assertFalse(isRowSet);

        checkCacheContents(TBL_NAME, true, 2);
    }

    /**
     * Verifies that COPY command can be called with PreparedStatement.executeUpdate().
     *
     * @throws SQLException If failed.
     */
    public void testPreparedStatementWithExecuteUpdate() throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement(BASIC_SQL_COPY_STMT);

        int updatesCnt = pstmt.executeUpdate();

        assertEquals(2, updatesCnt);

        checkCacheContents(TBL_NAME, true, 2);
    }

    /**
     * Verifies that COPY command reports an error when used with PreparedStatement parameter.
     */
    public void testPreparedStatementWithParameter() {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    PreparedStatement pstmt = conn.prepareStatement(
                        "copy from \"" + BULKLOAD_TWO_LINES_CSV_FILE + "\" into " + TBL_NAME +
                        " (_key, age, firstName, lastName)" +
                        " format ?");

                    pstmt.setString(1, "csv");

                    pstmt.executeUpdate();

                    return null;
                }
            }, SQLException.class, "Unexpected token: \"?\" (expected: \"[identifier]\"");
    }

    /**
     * Verifies that COPY command can be called with PreparedStatement.execute().
     *
     * @throws SQLException If failed.
     */
    public void testPreparedStatementWithExecute() throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement(BASIC_SQL_COPY_STMT);

        boolean isRowSet = pstmt.execute();

        assertFalse(isRowSet);

        checkCacheContents(TBL_NAME, true, 2);
    }

    /**
     * Verifies that COPY command is rejected by PreparedStatement.executeQuery().
     */
    public void testPreparedStatementWithExecuteQuery() {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                PreparedStatement pstmt = conn.prepareStatement(BASIC_SQL_COPY_STMT);

                pstmt.executeQuery();

                return null;
            }
        }, SQLException.class, "The query isn't SELECT query");
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
