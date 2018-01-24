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
import java.util.Collections;
import java.util.concurrent.Callable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_PARSER_DISABLE_H2_FALLBACK;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Statement test.
 */
public class JdbcThinBulkLoadSelfTest extends JdbcThinAbstractDmlStatementSelfTest {
    public static final String TBL_NAME = "Person";
    /** Statement. */
    private Statement stmt;

    private String BULKLOAD0_CSV_FILE = System.getProperty("IGNITE_HOME") +
        "/modules/clients/src/test/resources/bulkload2.csv";

    private String BULKLOAD_UTF_CSV_FILE = System.getProperty("IGNITE_HOME") +
        "/modules/clients/src/test/resources/bulkload2_utf.csv";

    private String BULKLOAD_ONE_LINE_CSV_FILE = System.getProperty("IGNITE_HOME") +
        "/modules/clients/src/test/resources/bulkload1.csv";

    private String BULKLOAD_EMPTY_CSV_FILE = System.getProperty("IGNITE_HOME") +
        "/modules/clients/src/test/resources/bulkload0.csv";

    @Override protected CacheConfiguration cacheConfig() {
        return cacheConfigWithIndexedTypes();
    }

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
     * @throws SQLException If failed.
     *
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
                    "copy from \"nonexistent\" into Person" +
                        " (_key, age, firstName, lastName)" +
                        " format csv");

                return null;
            }
        }, SQLException.class, "Failed to read file: 'nonexistent'");
    }

    /**
     * @throws SQLException If failed.
     *
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
     * @throws SQLException If failed.
     *
     */
    public void testBatchSize_1() throws SQLException {
        int updatesCnt = stmt.executeUpdate(
            "copy from \"" + BULKLOAD0_CSV_FILE + "\"" +
            " into " + TBL_NAME + " (_key, age, firstName, lastName) format csv batch_size 1");

        assertEquals(2, updatesCnt);

        checkCacheContents(TBL_NAME, true, 2);
    }

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
