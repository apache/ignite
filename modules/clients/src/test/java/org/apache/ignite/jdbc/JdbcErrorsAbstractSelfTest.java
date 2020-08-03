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

package org.apache.ignite.jdbc;

import java.io.Serializable;
import java.net.URL;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.cache.CacheInterceptorAdapter;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test SQLSTATE codes propagation with (any) Ignite JDBC driver.
 */
public abstract class JdbcErrorsAbstractSelfTest extends GridCommonAbstractTest {
    /** */
    protected static final String CACHE_STORE_TEMPLATE = "cache_store";

    /** */
    protected static final String CACHE_INTERCEPTOR_TEMPLATE = "cache_interceptor";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        IgniteEx grid = startGrid(getConfiguration(getTestIgniteInstanceName(0))
            .setCacheConfiguration(new CacheConfiguration("test")
                .setQueryEntities(Collections.singletonList(new QueryEntity(Integer.class, Integer.class)))));

        // add cache template for cache with enabled read-through cache store
        grid.addCacheConfiguration(new CacheConfiguration<>(CACHE_STORE_TEMPLATE)
            .setCacheStoreFactory(singletonFactory(new TestCacheStore())).setReadThrough(true));

        // add cache template for cache with enabled cache interceptor
        grid.addCacheConfiguration(new CacheConfiguration<>(CACHE_INTERCEPTOR_TEMPLATE)
            .setInterceptor(new TestCacheInterceptor()));
    }

    /** {@inheritDoc} */
    @Override protected boolean keepSerializedObjects() {
        return true;
    }

    /**
     * Test that H2 specific error codes get propagated to Ignite SQL exceptions.
     * @throws SQLException if failed.
     */
    @Test
    public void testParsingErrors() throws SQLException {
        checkErrorState("gibberish", "42000",
            "Failed to parse query. Syntax error in SQL statement \"GIBBERISH[*] \"");
    }

    /**
     * Test that error codes from tables related DDL operations get propagated to Ignite SQL exceptions.
     * @throws SQLException if failed.
     */
    @Test
    public void testTableErrors() throws SQLException {
        checkErrorState("DROP TABLE \"PUBLIC\".missing", "42000", "Table doesn't exist: MISSING");
    }

    /**
     * Test that error codes from indexes related DDL operations get propagated to Ignite SQL exceptions.
     * @throws SQLException if failed.
     */
    @Test
    public void testIndexErrors() throws SQLException {
        checkErrorState("DROP INDEX \"PUBLIC\".missing", "42000", "Index doesn't exist: MISSING");
    }

    /**
     * Test that error codes from DML operations get propagated to Ignite SQL exceptions.
     * @throws SQLException if failed.
     */
    @Test
    public void testDmlErrors() throws SQLException {
        checkErrorState("INSERT INTO \"test\".INTEGER(_key, _val) values(1, null)", "22004",
            "Value for INSERT, COPY, MERGE, or UPDATE must not be null");

        checkErrorState("INSERT INTO \"test\".INTEGER(_key, _val) values(1, 'zzz')", "0700B",
            "Value conversion failed [column=_VAL, from=java.lang.String, to=java.lang.Integer]");
    }

    /**
     * Test error code for the case when user attempts to refer a future currently unsupported.
     * @throws SQLException if failed.
     */
    @Test
    public void testUnsupportedSql() throws SQLException {
        checkErrorState("ALTER TABLE \"test\".Integer MODIFY COLUMN _key CHAR", "0A000",
            "ALTER COLUMN is not supported");
    }

    /**
     * Test error code for the case when user attempts to use a closed connection.
     * @throws SQLException if failed.
     */
    @Test
    public void testConnectionClosed() throws SQLException {
        checkErrorState(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                Connection conn = getConnection();

                conn.close();

                conn.prepareStatement("SELECT 1");

                return null;
            }
        }, "08003", "Connection is closed.");

        checkErrorState(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                Connection conn = getConnection();

                conn.close();

                conn.createStatement();

                return null;
            }
        }, "08003", "Connection is closed.");

        checkErrorState(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                Connection conn = getConnection();

                conn.close();

                conn.getMetaData();

                return null;
            }
        }, "08003", "Connection is closed.");

        checkErrorState(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                Connection conn = getConnection();

                DatabaseMetaData meta = conn.getMetaData();

                conn.close();

                meta.getIndexInfo(null, null, null, false, false);

                return null;
            }
        }, "08003", "Connection is closed.");

        checkErrorState(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                Connection conn = getConnection();

                DatabaseMetaData meta = conn.getMetaData();

                conn.close();

                meta.getColumns(null, null, null, null);

                return null;
            }
        }, "08003", "Connection is closed.");

        checkErrorState(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                Connection conn = getConnection();

                DatabaseMetaData meta = conn.getMetaData();

                conn.close();

                meta.getPrimaryKeys(null, null, null);

                return null;
            }
        }, "08003", "Connection is closed.");

        checkErrorState(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                Connection conn = getConnection();

                DatabaseMetaData meta = conn.getMetaData();

                conn.close();

                meta.getSchemas(null, null);

                return null;
            }
        }, "08003", "Connection is closed.");

        checkErrorState(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                Connection conn = getConnection();

                DatabaseMetaData meta = conn.getMetaData();

                conn.close();

                meta.getTables(null, null, null, null);

                return null;
            }
        }, "08003", "Connection is closed.");
    }

    /**
     * Test error code for the case when user attempts to use a closed result set.
     * @throws SQLException if failed.
     */
    @Test
    public void testResultSetClosed() throws SQLException {
        checkErrorState(new ConnClosure() {
            @Override public void run(Connection conn) throws Exception {
                try (PreparedStatement stmt = conn.prepareStatement("SELECT 1")) {
                    ResultSet rs = stmt.executeQuery();

                    rs.next();

                    rs.close();

                    rs.getInt(1);
                }
            }
        }, "24000", "Result set is closed");
    }

    /**
     * Test error code for the case when user attempts to get {@code int} value
     * from column whose value can't be converted to an {@code int}.
     * @throws SQLException if failed.
     */
    @Test
    public void testInvalidIntFormat() throws SQLException {
        checkErrorState(new ConnClosure() {
            @Override public void run(Connection conn) throws Exception {
                try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                    ResultSet rs = stmt.executeQuery();

                    rs.next();

                    rs.getInt(1);
                }
            }
        }, "0700B", "Cannot convert to int");
    }

    /**
     * Test error code for the case when user attempts to get {@code long} value
     * from column whose value can't be converted to an {@code long}.
     * @throws SQLException if failed.
     */
    @Test
    public void testInvalidLongFormat() throws SQLException {
        checkErrorState(new ConnClosure() {
            @Override public void run(Connection conn) throws Exception {
                try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                    ResultSet rs = stmt.executeQuery();

                    rs.next();

                    rs.getLong(1);
                }
            }
        }, "0700B", "Cannot convert to long");
    }

    /**
     * Test error code for the case when user attempts to get {@code float} value
     * from column whose value can't be converted to an {@code float}.
     * @throws SQLException if failed.
     */
    @Test
    public void testInvalidFloatFormat() throws SQLException {
        checkErrorState(new ConnClosure() {
            @Override public void run(Connection conn) throws Exception {
                try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                    ResultSet rs = stmt.executeQuery();

                    rs.next();

                    rs.getFloat(1);
                }
            }
        }, "0700B", "Cannot convert to float");
    }

    /**
     * Test error code for the case when user attempts to get {@code double} value
     * from column whose value can't be converted to an {@code double}.
     * @throws SQLException if failed.
     */
    @Test
    public void testInvalidDoubleFormat() throws SQLException {
        checkErrorState(new ConnClosure() {
            @Override public void run(Connection conn) throws Exception {
                try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                    ResultSet rs = stmt.executeQuery();

                    rs.next();

                    rs.getDouble(1);
                }
            }
        }, "0700B", "Cannot convert to double");
    }

    /**
     * Test error code for the case when user attempts to get {@code byte} value
     * from column whose value can't be converted to an {@code byte}.
     * @throws SQLException if failed.
     */
    @Test
    public void testInvalidByteFormat() throws SQLException {
        checkErrorState(new ConnClosure() {
            @Override public void run(Connection conn) throws Exception {
                try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                    ResultSet rs = stmt.executeQuery();

                    rs.next();

                    rs.getByte(1);
                }
            }
        }, "0700B", "Cannot convert to byte");
    }

    /**
     * Test error code for the case when user attempts to get {@code short} value
     * from column whose value can't be converted to an {@code short}.
     * @throws SQLException if failed.
     */
    @Test
    public void testInvalidShortFormat() throws SQLException {
        checkErrorState(new ConnClosure() {
            @Override public void run(Connection conn) throws Exception {
                try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                    ResultSet rs = stmt.executeQuery();

                    rs.next();

                    rs.getShort(1);
                }
            }
        }, "0700B", "Cannot convert to short");
    }

    /**
     * Test error code for the case when user attempts to get {@code BigDecimal} value
     * from column whose value can't be converted to an {@code BigDecimal}.
     * @throws SQLException if failed.
     */
    @Test
    public void testInvalidBigDecimalFormat() throws SQLException {
        checkErrorState(new ConnClosure() {
            @Override public void run(Connection conn) throws Exception {
                try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                    ResultSet rs = stmt.executeQuery();

                    rs.next();

                    rs.getBigDecimal(1);
                }
            }
        }, "0700B", "Cannot convert to");
    }

    /**
     * Test error code for the case when user attempts to get {@code boolean} value
     * from column whose value can't be converted to an {@code boolean}.
     * @throws SQLException if failed.
     */
    @Test
    public void testInvalidBooleanFormat() throws SQLException {
        checkErrorState(new ConnClosure() {
            @Override public void run(Connection conn) throws Exception {
                try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                    ResultSet rs = stmt.executeQuery();

                    rs.next();

                    rs.getBoolean(1);
                }
            }
        }, "0700B", "Cannot convert to boolean");
    }

    /**
     * Test error code for the case when user attempts to get {@code boolean} value
     * from column whose value can't be converted to an {@code boolean}.
     * @throws SQLException if failed.
     */
    @Test
    public void testInvalidObjectFormat() throws SQLException {
        checkErrorState(new ConnClosure() {
            @Override public void run(Connection conn) throws Exception {
                try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                    ResultSet rs = stmt.executeQuery();

                    rs.next();

                    rs.getObject(1, List.class);
                }
            }
        }, "0700B", "Cannot convert to");
    }

    /**
     * Test error code for the case when user attempts to get {@link Date} value
     * from column whose value can't be converted to a {@link Date}.
     * @throws SQLException if failed.
     */
    @Test
    public void testInvalidDateFormat() throws SQLException {
        checkErrorState(new ConnClosure() {
            @Override public void run(Connection conn) throws Exception {
                try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                    ResultSet rs = stmt.executeQuery();

                    rs.next();

                    rs.getDate(1);
                }
            }
        }, "0700B", "Cannot convert to date");
    }

    /**
     * Test error code for the case when user attempts to get {@link Time} value
     * from column whose value can't be converted to a {@link Time}.
     * @throws SQLException if failed.
     */
    @Test
    public void testInvalidTimeFormat() throws SQLException {
        checkErrorState(new ConnClosure() {
            @Override public void run(Connection conn) throws Exception {
                try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                    ResultSet rs = stmt.executeQuery();

                    rs.next();

                    rs.getTime(1);
                }
            }
        }, "0700B", "Cannot convert to time");
    }

    /**
     * Test error code for the case when user attempts to get {@link Timestamp} value
     * from column whose value can't be converted to a {@link Timestamp}.
     * @throws SQLException if failed.
     */
    @Test
    public void testInvalidTimestampFormat() throws SQLException {
        checkErrorState(new ConnClosure() {
            @Override public void run(Connection conn) throws Exception {
                try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                    ResultSet rs = stmt.executeQuery();

                    rs.next();

                    rs.getTimestamp(1);
                }
            }
        }, "0700B", "Cannot convert to timestamp");
    }

    /**
     * Test error code for the case when user attempts to get {@link URL} value
     * from column whose value can't be converted to a {@link URL}.
     * @throws SQLException if failed.
     */
    @Test
    public void testInvalidUrlFormat() throws SQLException {
        checkErrorState(new ConnClosure() {
            @Override public void run(Connection conn) throws Exception {
                try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                    ResultSet rs = stmt.executeQuery();

                    rs.next();

                    rs.getURL(1);
                }
            }
        }, "0700B", "Cannot convert to");
    }

    /**
     * Check error code for the case null value is inserted into table field declared as NOT NULL.
     *
     * @throws SQLException if failed.
     */
    @Test
    public void testNotNullViolation() throws SQLException {
        try (Connection conn = getConnection()) {
            conn.setSchema("PUBLIC");

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE nulltest(id INT PRIMARY KEY, name CHAR NOT NULL)");

                try {
                    checkErrorState(new IgniteCallable<Void>() {
                        @Override public Void call() throws Exception {
                            stmt.execute("INSERT INTO nulltest(id, name) VALUES (1, NULLIF('a', 'a'))");

                            return null;
                        }
                    }, "22004", "Null value is not allowed for column 'NAME'");
                }
                finally {
                    stmt.execute("DROP TABLE nulltest");
                }
            }
        }
    }

    /**
     * Check error code for the case not null field is configured for table belonging to cache
     * with enabled read-through cache store.
     *
     * @throws SQLException if failed.
     */
    @Test
    public void testNotNullRestrictionReadThroughCacheStore() throws SQLException {
        checkErrorState(new ConnClosure() {
            @Override public void run(Connection conn) throws Exception {
                conn.setSchema("PUBLIC");

                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("CREATE TABLE cache_store_nulltest(id INT PRIMARY KEY, age INT NOT NULL) " +
                        "WITH \"template=" + CACHE_STORE_TEMPLATE + "\"");
                }
            }
        }, "0A000",
            "NOT NULL constraint is not supported when CacheConfiguration.readThrough is enabled.");
    }

    /**
     * Check error code for the case not null field is configured for table belonging to cache
     * with configured cache interceptor.
     *
     * @throws SQLException if failed.
     */
    @Test
    public void testNotNullRestrictionCacheInterceptor() throws SQLException {
        checkErrorState(new ConnClosure() {
            @Override public void run(Connection conn) throws Exception {
                conn.setSchema("PUBLIC");

                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("CREATE TABLE cache_interceptor_nulltest(id INT PRIMARY KEY, age INT NOT NULL) " +
                        "WITH \"template=" + CACHE_INTERCEPTOR_TEMPLATE + "\"");
                }
            }
        }, "0A000", "NOT NULL constraint is not supported when CacheConfiguration.interceptor is set.");
    }

    /**
     * Checks wrong table name select error message.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testSelectWrongTable() throws SQLException {
        checkSqlErrorMessage("select from wrong", "42000",
            "Failed to parse query. Table \"WRONG\" not found");
    }

    /**
     * Checks wrong column name select error message.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testSelectWrongColumnName() throws SQLException {
        checkSqlErrorMessage("select wrong from test", "42000",
            "Failed to parse query. Column \"WRONG\" not found");
    }

    /**
     * Checks wrong syntax select error message.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testSelectWrongSyntax() throws SQLException {
        checkSqlErrorMessage("select from test where", "42000",
            "Failed to parse query. Syntax error in SQL statement \"SELECT FROM TEST WHERE[*]");
    }

    /**
     * Checks wrong table name DML error message.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testDmlWrongTable() throws SQLException {
        checkSqlErrorMessage("insert into wrong (id, val) values (3, 'val3')", "42000",
            "Failed to parse query. Table \"WRONG\" not found");

        checkSqlErrorMessage("merge into wrong (id, val) values (3, 'val3')", "42000",
            "Failed to parse query. Table \"WRONG\" not found");

        checkSqlErrorMessage("update wrong set val = 'val3' where id = 2", "42000",
            "Failed to parse query. Table \"WRONG\" not found");

        checkSqlErrorMessage("delete from wrong where id = 2", "42000",
            "Failed to parse query. Table \"WRONG\" not found");
    }

    /**
     * Checks wrong column name DML error message.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testDmlWrongColumnName() throws SQLException {
        checkSqlErrorMessage("insert into test (id, wrong) values (3, 'val3')", "42000",
            "Failed to parse query. Column \"WRONG\" not found");

        checkSqlErrorMessage("merge into test (id, wrong) values (3, 'val3')", "42000",
            "Failed to parse query. Column \"WRONG\" not found");

        checkSqlErrorMessage("update test set wrong = 'val3' where id = 2", "42000",
            "Failed to parse query. Column \"WRONG\" not found");

        checkSqlErrorMessage("delete from test where wrong = 2", "42000",
            "Failed to parse query. Column \"WRONG\" not found");
    }

    /**
     * Checks wrong syntax DML error message.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testDmlWrongSyntax() throws SQLException {
        checkSqlErrorMessage("insert test (id, val) values (3, 'val3')", "42000",
            "Failed to parse query. Syntax error in SQL statement \"INSERT TEST[*] (ID, VAL)");

        checkSqlErrorMessage("merge test (id, val) values (3, 'val3')", "42000",
            "Failed to parse query. Syntax error in SQL statement \"MERGE TEST[*] (ID, VAL)");

        checkSqlErrorMessage("update test val = 'val3' where id = 2", "42000",
            "Failed to parse query. Syntax error in SQL statement \"UPDATE TEST VAL =[*] 'val3' WHERE ID = 2");

        checkSqlErrorMessage("delete from test 1where id = 2", "42000",
            "Failed to parse query. Syntax error in SQL statement \"DELETE FROM TEST 1[*]WHERE ID = 2 ");
    }

    /**
     * Checks wrong table name DDL error message.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testDdlWrongTable() throws SQLException {
        checkSqlErrorMessage("create table test (id int primary key, val varchar)", "42000",
            "Table already exists: TEST");

        checkSqlErrorMessage("drop table wrong", "42000",
            "Table doesn't exist: WRONG");

        checkSqlErrorMessage("create index idx1 on wrong (val)", "42000",
            "Table doesn't exist: WRONG");

        checkSqlErrorMessage("drop index wrong", "42000",
            "Index doesn't exist: WRONG");

        checkSqlErrorMessage("alter table wrong drop column val", "42000",
            "Failed to parse query. Table \"WRONG\" not found");
    }

    /**
     * Checks wrong column name DDL error message.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testDdlWrongColumnName() throws SQLException {
        checkSqlErrorMessage("create index idx1 on test (wrong)", "42000",
            "Column doesn't exist: WRONG");

        checkSqlErrorMessage("alter table test drop column wrong", "42000",
            "Failed to parse query. Column \"WRONG\" not found");

        checkSqlErrorMessage("create table test(id integer primary key, AgE integer, AGe integer)", "42000",
            "Duplicate column name: AGE");

        checkSqlErrorMessage("create table test(\"id\" integer primary key, \"age\" integer, \"age\" integer)", "42000",
            "Duplicate column name: age");

        checkSqlErrorMessage("create table test(id integer primary key, age integer, age varchar)", "42000",
            "Duplicate column name: AGE");
    }

    /**
     * Checks wrong syntax DDL error message.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testDdlWrongSyntax() throws SQLException {
        checkSqlErrorMessage("create table test2 (id int wrong key, val varchar)", "42000",
            "Failed to parse query. Syntax error in SQL statement \"CREATE TABLE TEST2 (ID INT WRONG[*]");

        checkSqlErrorMessage("drop table test on", "42000",
            "Failed to parse query. Syntax error in SQL statement \"DROP TABLE TEST ON[*]");

        checkSqlErrorMessage("create index idx1 test (val)", "42000",
            "Failed to parse query. Syntax error in SQL statement \"CREATE INDEX IDX1 TEST[*]");

        checkSqlErrorMessage("drop index", "42000",
            "Failed to parse query. Syntax error in SQL statement \"DROP INDEX [*]");

        checkSqlErrorMessage("alter table test drop column", "42000",
            "Failed to parse query. Syntax error in SQL statement \"ALTER TABLE TEST DROP COLUMN [*]");
    }

    /**
     * Checks execution DML request on read-only cluster error code and message.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUpdatesRejectedInReadOnlyMode() throws Exception {
        try (Connection conn = getConnection()) {
            try (Statement statement = conn.createStatement()) {
                statement.executeUpdate("CREATE TABLE TEST_READ_ONLY (ID LONG PRIMARY KEY, VAL LONG)");
            }
        }

        grid(0).cluster().state(ClusterState.ACTIVE_READ_ONLY);

        try {
            checkErrorState((conn) -> {
                try (Statement statement = conn.createStatement()) {
                    statement.executeUpdate("INSERT INTO TEST_READ_ONLY VALUES (1, 2)");
                }
            }, "90097", "Failed to execute DML statement. Cluster in read-only mode");
        }
        finally {
            grid(0).cluster().state(ClusterState.ACTIVE);
        }
    }

    /**
     * Checks execution batch DML request on read-only cluster error code and message.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBatchUpdatesRejectedInReadOnlyMode() throws Exception {
        try (Connection conn = getConnection()) {
            try (Statement statement = conn.createStatement()) {
                statement.executeUpdate("CREATE TABLE TEST_READ_ONLY_BATCH (ID LONG PRIMARY KEY, VAL LONG)");
            }
        }

        grid(0).cluster().state(ClusterState.ACTIVE_READ_ONLY);

        try {
            checkErrorState((conn) -> {
                try (Statement statement = conn.createStatement()) {
                    statement.addBatch("INSERT INTO TEST_READ_ONLY_BATCH VALUES (1, 2)");
                    statement.executeBatch();
                }
            }, "90097", null);
        }
        finally {
            grid(0).cluster().state(ClusterState.ACTIVE);
        }
    }

    /**
     * @return Connection to execute statements on.
     * @throws SQLException if failed.
     */
    protected abstract Connection getConnection() throws SQLException;

    /**
     * Test that running given SQL statement yields expected SQLSTATE code.
     * @param sql statement.
     * @param expState expected SQLSTATE code.
     * @throws SQLException if failed.
     */
    private void checkErrorState(final String sql, String expState, String expMsg) throws SQLException {
        checkErrorState(new ConnClosure() {
            @Override public void run(Connection conn) throws Exception {
                try (final PreparedStatement stmt = conn.prepareStatement(sql)) {
                    stmt.execute();
                }
            }
        }, expState, expMsg);
    }

    /**
     * Test that running given closure yields expected SQLSTATE code.
     * @param clo closure.
     * @param expState expected SQLSTATE code.
     * @throws SQLException if failed.
     */
    protected void checkErrorState(final ConnClosure clo, String expState, String expMsg) throws SQLException {
        checkErrorState(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                try (final Connection conn = getConnection()) {
                    clo.run(conn);

                    fail();

                    return null;
                }
            }
        }, expState, expMsg);
    }

    /**
     * Test that running given closure yields expected SQLSTATE code.
     * @param clo closure.
     * @param expState expected SQLSTATE code.
     * @throws SQLException if failed.
     */
    protected void checkErrorState(final IgniteCallable<Void> clo, String expState, String expMsg) throws SQLException {
        SQLException ex = (SQLException)GridTestUtils.assertThrows(null, clo, SQLException.class, expMsg);

        assertEquals(expState, ex.getSQLState());
    }

    /**
     * Check SQL exception message and error code.
     *
     * @param sql Query string.
     * @param expState Error code.
     * @param expMsg Error message.
     * @throws SQLException if failed.
     */
    private void checkSqlErrorMessage(final String sql, String expState, String expMsg) throws SQLException {
        checkErrorState(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                try (final Connection conn = getConnection()) {
                    conn.setSchema("PUBLIC");

                    try (Statement stmt = conn.createStatement()) {
                        stmt.executeUpdate("DROP TABLE IF EXISTS wrong");
                        stmt.executeUpdate("DROP TABLE IF EXISTS test");

                        stmt.executeUpdate("CREATE TABLE test (id INT PRIMARY KEY, val VARCHAR)");

                        stmt.executeUpdate("INSERT INTO test (id, val) VALUES (1, 'val1')");
                        stmt.executeUpdate("INSERT INTO test (id, val) VALUES (2, 'val2')");

                        stmt.execute(sql);

                        fail("Exception is expected");
                    }

                    return null;
                }
            }
        }, expState, expMsg);
    }

    /**
     * Runnable that accepts a {@link Connection} and can throw an exception.
     */
    protected interface ConnClosure {
        /**
         * @throws Exception On error.
         */
        void run(Connection conn) throws Exception;
    }

    /**
     * Cache store stub.
     */
    protected class TestCacheStore extends CacheStoreAdapter<Object,Object> implements Serializable {
        /** {@inheritDoc} */
        @Override public Object load(Object key) throws CacheLoaderException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<?, ?> entry) throws CacheWriterException {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            // No-op
        }
    }

    /**
     * Cache interceptor stub.
     */
    private static class TestCacheInterceptor extends CacheInterceptorAdapter<Object, Object> implements Serializable {
        // No-op
    }
}
