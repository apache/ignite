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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.apache.ignite.jdbc.IgniteJdbcDriver;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static java.sql.Connection.TRANSACTION_NONE;
import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_READ_UNCOMMITTED;
import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;
import static java.sql.Connection.TRANSACTION_SERIALIZABLE;
import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.sql.Statement.NO_GENERATED_KEYS;
import static java.sql.Statement.RETURN_GENERATED_KEYS;
import static org.apache.ignite.client.proto.query.SqlStateCode.TRANSACTION_STATE_EXCEPTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Connection test.
 */
@SuppressWarnings("ThrowableNotThrown")
public class ITJdbcConnectionSelfTest extends AbstractJdbcSelfTest {
    /**
     * Test JDBC loading via ServiceLoader
     */
    @Test
    public void testServiceLoader() {
        ServiceLoader<Driver> sl = ServiceLoader.load(Driver.class);

        IgniteJdbcDriver igniteJdbcDriver = null;

        for (Driver driver : sl) {
            if (driver instanceof IgniteJdbcDriver) {
                igniteJdbcDriver = ((IgniteJdbcDriver)driver);
                break;
            }
        }

        assertNotNull(igniteJdbcDriver);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"EmptyTryBlock", "unused"})
    @Test
    public void testDefaults() throws Exception {
        var url = "jdbc:ignite:thin://127.0.1.1:10800";

        try (Connection conn = DriverManager.getConnection(url)) {
            // No-op.
        }

        try (Connection conn = DriverManager.getConnection(url + "/")) {
            // No-op.
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"EmptyTryBlock", "unused"})
    @Test
    @Disabled("ITDS-1887")
    public void testDefaultsIPv6() throws Exception {
        var url = "jdbc:ignite:thin://[::1]:10800";

        try (Connection conn = DriverManager.getConnection(url)) {
            // No-op.
        }

        try (Connection conn = DriverManager.getConnection(url + "/")) {
            // No-op.
        }
    }

    /**
     * Test invalid endpoint.
     */
    @Test
    public void testInvalidEndpoint() {
        assertInvalid("jdbc:ignite:thin://", "Address is empty");
        assertInvalid("jdbc:ignite:thin://:10000", "Host name is empty");
        assertInvalid("jdbc:ignite:thin://     :10000", "Host name is empty");

        assertInvalid("jdbc:ignite:thin://127.0.0.1:-1", "port range contains invalid port -1");
        assertInvalid("jdbc:ignite:thin://127.0.0.1:0", "port range contains invalid port 0");
        assertInvalid("jdbc:ignite:thin://127.0.0.1:100000",
            "port range contains invalid port 100000");

        assertInvalid("jdbc:ignite:thin://[::1]:-1", "port range contains invalid port -1");
        assertInvalid("jdbc:ignite:thin://[::1]:0", "port range contains invalid port 0");
        assertInvalid("jdbc:ignite:thin://[::1]:100000",
            "port range contains invalid port 100000");
    }

    /**
     * Test schema property in URL.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSchema() throws Exception {
        assertInvalid(URL + "/qwe/qwe",
            "Invalid URL format (only schema name is allowed in URL path parameter 'host:port[/schemaName]')" );

        try (Connection conn = DriverManager.getConnection(URL + "/public")) {
            assertEquals( "PUBLIC", conn.getSchema(), "Invalid schema");
        }

        String dfltSchema = "DEFAULT";

        try (Connection conn = DriverManager.getConnection(URL + "/\"" + dfltSchema + '"')) {
            assertEquals(dfltSchema, conn.getSchema(), "Invalid schema");
        }

        try (Connection conn = DriverManager.getConnection(URL + "/_not_exist_schema_")) {
            assertEquals("_NOT_EXIST_SCHEMA_", conn.getSchema(), "Invalid schema");
        }
    }

    /**
     * Test schema property in URL with semicolon.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSchemaSemicolon() throws Exception {
        String dfltSchema = "DEFAULT";

        try (Connection conn = DriverManager.getConnection(URL + ";schema=public")) {
            assertEquals("PUBLIC", conn.getSchema(), "Invalid schema");
        }

        try (Connection conn =
                 DriverManager.getConnection(URL + ";schema=\"" + dfltSchema + '"')) {
            assertEquals( dfltSchema, conn.getSchema(), "Invalid schema");
        }

        try (Connection conn = DriverManager.getConnection(URL + ";schema=_not_exist_schema_")) {
            assertEquals("_NOT_EXIST_SCHEMA_", conn.getSchema(), "Invalid schema");
        }
    }

    /**
     * Assert that provided URL is invalid.
     *
     * @param url URL.
     * @param errMsg Error message.
     */
    private void assertInvalid(final String url, String errMsg) {
        assertThrows(SQLException.class, () -> DriverManager.getConnection(url), errMsg);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClose() throws Exception {
        final Connection conn;

        try (Connection conn0 = DriverManager.getConnection(URL)) {
            conn = conn0;

            assertNotNull(conn);
            assertFalse(conn.isClosed());
        }

        assertTrue(conn.isClosed());

        assertFalse(conn.isValid(2), "Connection must be closed");

        assertThrows(SQLException.class, () -> conn.isValid(-2), "Invalid timeout");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateStatement() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            try (Statement stmt = conn.createStatement()) {
                assertNotNull(stmt);

                stmt.close();

                conn.close();

                // Exception when called on closed connection
                checkConnectionClosed(() -> conn.createStatement());
            }
        }
    }

    /**
     * TODO  IGNITE-15188
     *
     * @throws Exception If failed.
     */
    @Test
    @Disabled
    public void testCreateStatement2() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            int[] rsTypes = new int[]
                {TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE};

            int[] rsConcurs = new int[]
                {CONCUR_READ_ONLY, ResultSet.CONCUR_UPDATABLE};

            DatabaseMetaData meta = conn.getMetaData();

            for (final int type : rsTypes) {
                for (final int concur : rsConcurs) {
                    if (meta.supportsResultSetConcurrency(type, concur)) {
                        assertEquals(TYPE_FORWARD_ONLY, type);
                        assertEquals(CONCUR_READ_ONLY, concur);

                        try (Statement stmt = conn.createStatement(type, concur)) {
                            assertNotNull(stmt);

                            assertEquals(type, stmt.getResultSetType());
                            assertEquals(concur, stmt.getResultSetConcurrency());
                        }

                        continue;
                    }

                    assertThrows(SQLFeatureNotSupportedException.class, () -> conn.createStatement(type, concur));
                }
            }

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(() -> conn.createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY));
        }
    }

    /**
     * TODO  IGNITE-15188
     *
     * @throws Exception If failed.
     */
    @Test
    @Disabled
    public void testCreateStatement3() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            int[] rsTypes = new int[]
                {TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE};

            int[] rsConcurs = new int[]
                {CONCUR_READ_ONLY, ResultSet.CONCUR_UPDATABLE};

            int[] rsHoldabilities = new int[]
                {HOLD_CURSORS_OVER_COMMIT, CLOSE_CURSORS_AT_COMMIT};

            DatabaseMetaData meta = conn.getMetaData();

            for (final int type : rsTypes) {
                for (final int concur : rsConcurs) {
                    for (final int holdabililty : rsHoldabilities) {
                        if (meta.supportsResultSetConcurrency(type, concur)) {
                            assertEquals(TYPE_FORWARD_ONLY, type);
                            assertEquals(CONCUR_READ_ONLY, concur);

                            try (Statement stmt = conn.createStatement(type, concur, holdabililty)) {
                                assertNotNull(stmt);

                                assertEquals(type, stmt.getResultSetType());
                                assertEquals(concur, stmt.getResultSetConcurrency());
                                assertEquals(holdabililty, stmt.getResultSetHoldability());
                            }

                            continue;
                        }

                        assertThrows(SQLFeatureNotSupportedException.class, 
                            () -> conn.createStatement(type, concur, holdabililty));
                    }
                }
            }

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(() -> conn.createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPrepareStatement() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            // null query text
            assertThrows(
                NullPointerException.class,
                () -> conn.prepareStatement(null)
            );

            final String sqlText = "select * from test where param = ?";

            try (PreparedStatement prepared = conn.prepareStatement(sqlText)) {
                assertNotNull(prepared);
            }

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(() -> conn.prepareStatement(sqlText));
        }
    }

    /**
     * TODO  IGNITE-15188
     *
     * @throws Exception If failed.
     */
    @Test
    @Disabled
    public void testPrepareStatement3() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            final String sqlText = "select * from test where param = ?";

            int[] rsTypes = new int[]
                {TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE};

            int[] rsConcurs = new int[]
                {CONCUR_READ_ONLY, ResultSet.CONCUR_UPDATABLE};

            DatabaseMetaData meta = conn.getMetaData();

            for (final int type : rsTypes) {
                for (final int concur : rsConcurs) {
                    if (meta.supportsResultSetConcurrency(type, concur)) {
                        assertEquals(TYPE_FORWARD_ONLY, type);
                        assertEquals(CONCUR_READ_ONLY, concur);

                        // null query text
                        assertThrows(
                            SQLException.class,
                            () -> conn.prepareStatement(null, type, concur),
                            "SQL string cannot be null"
                        );

                        continue;
                    }

                    assertThrows(
                        SQLFeatureNotSupportedException.class,
                        () -> conn.prepareStatement(sqlText, type, concur)
                    );
                }
            }

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(() -> conn.prepareStatement(sqlText, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY));

            conn.close();
        }
    }

    /**
     * TODO  IGNITE-15188
     *
     * @throws Exception If failed.
     */
    @Test
    @Disabled
    public void testPrepareStatement4() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            final String sqlText = "select * from test where param = ?";

            int[] rsTypes = new int[]
                {TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE};

            int[] rsConcurs = new int[]
                {CONCUR_READ_ONLY, ResultSet.CONCUR_UPDATABLE};

            int[] rsHoldabilities = new int[]
                {HOLD_CURSORS_OVER_COMMIT, CLOSE_CURSORS_AT_COMMIT};

            DatabaseMetaData meta = conn.getMetaData();

            for (final int type : rsTypes) {
                for (final int concur : rsConcurs) {
                    for (final int holdabililty : rsHoldabilities) {
                        if (meta.supportsResultSetConcurrency(type, concur)) {
                            assertEquals(TYPE_FORWARD_ONLY, type);
                            assertEquals(CONCUR_READ_ONLY, concur);

                            // null query text
                            assertThrows(
                                SQLException.class,
                                () -> conn.prepareStatement(null, type, concur, holdabililty),
                                "SQL string cannot be null"
                            );

                            continue;
                        }

                        assertThrows(
                            SQLFeatureNotSupportedException.class,
                            () -> conn.prepareStatement(sqlText, type, concur, holdabililty)
                        );
                    }
                }
            }

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(
                () -> conn.prepareStatement(sqlText, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT)
            );

            conn.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPrepareStatementAutoGeneratedKeysUnsupported() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            final String sqlText = "insert into test (val) values (?)";

            assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> conn.prepareStatement(sqlText, RETURN_GENERATED_KEYS),
                "Auto generated keys are not supported."
            );

            assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> conn.prepareStatement(sqlText, NO_GENERATED_KEYS),
                "Auto generated keys are not supported."
            );

            assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> conn.prepareStatement(sqlText, new int[] {1}),
                "Auto generated keys are not supported."
            );

            assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> conn.prepareStatement(sqlText, new String[] {"ID"}),
                "Auto generated keys are not supported."
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPrepareCallUnsupported() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            final String sqlText = "exec test()";

            assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> conn.prepareCall(sqlText),
                "Callable functions are not supported."
            );

            assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> conn.prepareCall(sqlText, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY),
                "Callable functions are not supported."
            );

            assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> conn.prepareCall(sqlText, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT),
                "Callable functions are not supported."
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNativeSql() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            // null query text
            assertThrows(
                NullPointerException.class,
                () -> conn.nativeSQL(null)
            );

            final String sqlText = "select * from test";

            assertEquals(sqlText, conn.nativeSQL(sqlText));

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(() -> conn.nativeSQL(sqlText));
        }
    }

    /**
     * TODO Enable when transactions are ready.
     *
     * @throws Exception If failed.
     */
    @Test
    @Disabled
    public void testGetSetAutoCommit() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            boolean ac0 = conn.getAutoCommit();

            conn.setAutoCommit(!ac0);
            // assert no exception

            conn.setAutoCommit(ac0);
            // assert no exception

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(() -> conn.setAutoCommit(ac0));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCommit() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            // Should not be called in auto-commit mode
            assertThrows(
                SQLException.class,
                () -> conn.commit(),
                "Transaction cannot be committed explicitly in auto-commit mode"
            );

            assertTrue(conn.getAutoCommit());

            // Should not be called in auto-commit mode
            assertThrows(
                SQLException.class,
                () -> conn.commit(),
                "Transaction cannot be committed explicitly in auto-commit mode."
            );

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(() -> conn.commit());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRollback() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            // Should not be called in auto-commit mode
            assertThrows(
                SQLException.class,
                () -> conn.rollback(),
                "Transaction cannot be rolled back explicitly in auto-commit mode."
            );

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(() -> conn.rollback());
        }
    }

    /**
     * Enable when transactions are ready.
     *
     * @throws Exception if failed.
     */
    @Test
    @Disabled
    public void testBeginFailsWhenMvccIsDisabled() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            conn.createStatement().execute("BEGIN");

            fail("Exception is expected");
        }
        catch (SQLException e) {
            assertEquals(TRANSACTION_STATE_EXCEPTION, e.getSQLState());
        }
    }

    /**
     * Enable when transactions are ready.
     *
     * @throws Exception if failed.
     */
    @Test
    @Disabled
    public void testCommitIgnoredWhenMvccIsDisabled() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            conn.setAutoCommit(false);
            conn.createStatement().execute("COMMIT");

            conn.commit();
        }
        // assert no exception
    }

    /**
     * Enable when transactions are ready.
     *
     * @throws Exception if failed.
     */
    @Test
    @Disabled
    public void testRollbackIgnoredWhenMvccIsDisabled() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            conn.setAutoCommit(false);

            conn.createStatement().execute("ROLLBACK");

            conn.rollback();
        }
        // assert no exception
    }

    /**
     * TODO  IGNITE-15188
     *
     * @throws Exception If failed.
     */
    @Test
    @Disabled
    public void testGetMetaData() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            assertNotNull(meta);

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(() -> conn.getMetaData());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetSetReadOnly() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(() -> conn.setReadOnly(true));

            // Exception when called on closed connection
            checkConnectionClosed(() -> conn.isReadOnly());
        }
    }

    /**
     * TODO  IGNITE-15188
     *
     * @throws Exception If failed.
     */
    @Test
    @Disabled
    public void testGetSetCatalog() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            assertFalse(conn.getMetaData().supportsCatalogsInDataManipulation());

            assertNull(conn.getCatalog());

            conn.setCatalog("catalog");

            assertNull(conn.getCatalog());

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(() -> conn.setCatalog(""));

            // Exception when called on closed connection
            checkConnectionClosed(() -> conn.getCatalog());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetSetTransactionIsolation() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            // Invalid parameter value
            assertThrows(
                SQLException.class,
                () -> conn.setTransactionIsolation(-1),
                "Invalid transaction isolation level"
            );

            // default level
            assertEquals(TRANSACTION_NONE, conn.getTransactionIsolation());

            int[] levels = {
                TRANSACTION_READ_UNCOMMITTED, TRANSACTION_READ_COMMITTED,
                TRANSACTION_REPEATABLE_READ, TRANSACTION_SERIALIZABLE};

            for (int level : levels) {
                conn.setTransactionIsolation(level);
                assertEquals(level, conn.getTransactionIsolation());
            }

            conn.close();

            // Exception when called on closed connection

            checkConnectionClosed(() -> conn.getTransactionIsolation());

            // Exception when called on closed connection
            checkConnectionClosed(() -> conn.setTransactionIsolation(TRANSACTION_SERIALIZABLE));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClearGetWarnings() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            SQLWarning warn = conn.getWarnings();

            assertNull(warn);

            conn.clearWarnings();

            warn = conn.getWarnings();

            assertNull(warn);

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(() -> conn.getWarnings());

            // Exception when called on closed connection
            checkConnectionClosed(() -> conn.clearWarnings());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetSetTypeMap() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> conn.getTypeMap(),
                "Types mapping is not supported"
            );

            assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> conn.setTypeMap(new HashMap<String, Class<?>>()),
                "Types mapping is not supported"
            );
            
            conn.close();
            
            // Exception when called on closed connection

            assertThrows(
                SQLException.class,
                () -> conn.getTypeMap(),
                "Connection is closed"
            );

            assertThrows(
                SQLException.class,
                () -> conn.setTypeMap(new HashMap<String, Class<?>>()),
                "Connection is closed"
            );
        }
    }

    /**
     * TODO  IGNITE-15188
     *
     * @throws Exception If failed.
     */
    @Test
    @Disabled
    public void testGetSetHoldability() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            // default value
            assertEquals(conn.getMetaData().getResultSetHoldability(), conn.getHoldability());

            assertEquals(HOLD_CURSORS_OVER_COMMIT, conn.getHoldability());

            conn.setHoldability(CLOSE_CURSORS_AT_COMMIT);

            assertEquals(CLOSE_CURSORS_AT_COMMIT, conn.getHoldability());

            // Invalid constant
            
            assertThrows(
                SQLException.class,
                () -> conn.setHoldability(-1),
                "Invalid result set holdability value"
            );

            conn.close();

            assertThrows(
                SQLException.class,
                () -> conn.getHoldability(),
                "Connection is closed"
            );

            assertThrows(
                SQLException.class,
                () -> conn.setHoldability(HOLD_CURSORS_OVER_COMMIT),
                "Connection is closed"
            );
        }
    }

    /**
     * TODO  IGNITE-15188
     *
     * @throws Exception If failed.
     */
    @Test
    @Disabled
    public void testSetSavepoint() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            assertFalse(conn.getMetaData().supportsSavepoints());

            // Disallowed in auto-commit mode
            assertThrows(
                SQLException.class,
                () -> conn.setSavepoint(),
                "Savepoint cannot be set in auto-commit mode"
            );

            conn.close();

            checkConnectionClosed(() -> conn.setSavepoint());
        }
    }

    /**
     * TODO  IGNITE-15188
     *
     * @throws Exception If failed.
     */
    @Test
    @Disabled
    public void testSetSavepointName() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            assertFalse(conn.getMetaData().supportsSavepoints());

            // Invalid arg
            assertThrows(
                SQLException.class,
                () -> conn.setSavepoint(null),
                "Savepoint name cannot be null"
            );

            final String name = "savepoint";

            // Disallowed in auto-commit mode
            assertThrows(
                SQLException.class,
                () -> conn.setSavepoint(name),
                "Savepoint cannot be set in auto-commit mode"
            );

            conn.close();

            checkConnectionClosed(() -> conn.setSavepoint(name));
        }
    }

    /**
     * TODO  IGNITE-15188
     *
     * @throws Exception If failed.
     */
    @Test
    @Disabled
    public void testRollbackSavePoint() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            assertFalse(conn.getMetaData().supportsSavepoints());

            // Invalid arg
            assertThrows(
                SQLException.class,
                () -> conn.rollback(null),
                "Invalid savepoint"
            );

            final Savepoint savepoint = getFakeSavepoint();

            // Disallowed in auto-commit mode
            assertThrows(
                SQLException.class,
                () -> conn.rollback(savepoint),
                "Auto-commit mode"
            );

            conn.close();

            checkConnectionClosed(() -> conn.rollback(savepoint));
        }
    }

    /**
     * TODO  IGNITE-15188
     *
     * @throws Exception If failed.
     */
    @Test
    @Disabled
    public void testReleaseSavepoint() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            assertFalse(conn.getMetaData().supportsSavepoints());

            // Invalid arg
            assertThrows(
                SQLException.class,
                () -> conn.releaseSavepoint(null),
                "Savepoint cannot be null"
            );

            final Savepoint savepoint = getFakeSavepoint();

            checkNotSupported(() -> conn.releaseSavepoint(savepoint));

            conn.close();

            checkConnectionClosed(() -> conn.releaseSavepoint(savepoint));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateClob() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            // Unsupported
            assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> conn.createClob(),
                "SQL-specific types are not supported"
            );

            conn.close();

            assertThrows(
                SQLException.class,
                () -> conn.createClob(),
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateBlob() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            // Unsupported
            assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> conn.createBlob(),
                "SQL-specific types are not supported"
            );

            conn.close();

            assertThrows(
                SQLException.class,
                () -> conn.createBlob(),
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateNClob() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            // Unsupported
            assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> conn.createNClob(),
                "SQL-specific types are not supported"
            );

            conn.close();

            assertThrows(
                SQLException.class,
                () -> conn.createNClob(),
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateSQLXML() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            // Unsupported
            assertThrows(
                SQLFeatureNotSupportedException.class,
                () -> conn.createSQLXML(),
                "SQL-specific types are not supported"
            );

            conn.close();

            assertThrows(
                SQLException.class,
                () -> conn.createSQLXML(),
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetSetClientInfoPair() throws Exception {
//        fail("https://issues.apache.org/jira/browse/IGNITE-5425");

        try (Connection conn = DriverManager.getConnection(URL)) {
            final String name = "ApplicationName";
            final String val = "SelfTest";

            assertNull(conn.getWarnings());

            conn.setClientInfo(name, val);

            assertNull(conn.getClientInfo(val));

            conn.close();

            checkConnectionClosed(() -> conn.getClientInfo(name));

            assertThrows(
                SQLClientInfoException.class,
                () -> conn.setClientInfo(name, val),
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetSetClientInfoProperties() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            final String name = "ApplicationName";
            final String val = "SelfTest";

            final Properties props = new Properties();
            props.setProperty(name, val);

            conn.setClientInfo(props);

            Properties propsResult = conn.getClientInfo();

            assertNotNull(propsResult);

            assertTrue(propsResult.isEmpty());

            conn.close();

            checkConnectionClosed(() -> conn.getClientInfo());

            assertThrows(
                SQLClientInfoException.class,
                () -> conn.setClientInfo(props),
                "Connection is closed"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateArrayOf() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            final String typeName = "varchar";

            final String[] elements = new String[] {"apple", "pear"};

            // Invalid typename
            assertThrows(
                SQLException.class,
                () -> conn.createArrayOf(null, null),
                "Type name cannot be null"
            );

            // Unsupported

            checkNotSupported(() -> conn.createArrayOf(typeName, elements));

            conn.close();

            checkConnectionClosed(() -> conn.createArrayOf(typeName, elements));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateStruct() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            // Invalid typename
            assertThrows(
                SQLException.class,
                () -> conn.createStruct(null, null),
                "Type name cannot be null"
            );

            final String typeName = "employee";

            final Object[] attrs = new Object[] {100, "Tom"};

            checkNotSupported(() -> conn.createStruct(typeName, attrs));

            conn.close();

            checkConnectionClosed(() -> conn.createStruct(typeName, attrs));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetSetSchema() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            assertEquals("PUBLIC", conn.getSchema());

            final String schema = "test";

            conn.setSchema(schema);

            assertEquals(schema.toUpperCase(), conn.getSchema());

            conn.setSchema('"' + schema + '"');

            assertEquals(schema, conn.getSchema());

            conn.close();

            checkConnectionClosed(() -> conn.setSchema(schema));

            checkConnectionClosed(() -> conn.getSchema());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAbort() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            //Invalid executor
            assertThrows(
                SQLException.class,
                () -> conn.abort(null),
                "Executor cannot be null"
            );

            final Executor executor = Executors.newFixedThreadPool(1);

            conn.abort(executor);

            assertTrue(conn.isClosed());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetSetNetworkTimeout() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            // default
            assertEquals(0, conn.getNetworkTimeout());

            final Executor executor = Executors.newFixedThreadPool(1);

            final int timeout = 1000;

            //Invalid timeout
            assertThrows(
                SQLException.class,
                () -> conn.setNetworkTimeout(executor, -1),
                "Network timeout cannot be negative"
            );

            conn.setNetworkTimeout(executor, timeout);

            assertEquals(timeout, conn.getNetworkTimeout());

            conn.close();

            checkConnectionClosed(() -> conn.getNetworkTimeout());

            checkConnectionClosed(() -> conn.setNetworkTimeout(executor, timeout));
        }
    }

    /**
     * @return Savepoint.
     */
    private Savepoint getFakeSavepoint() {
        return new Savepoint() {
            @Override public int getSavepointId() throws SQLException {
                return 100;
            }

            @Override public String getSavepointName() {
                return "savepoint";
            }
        };
    }
}
