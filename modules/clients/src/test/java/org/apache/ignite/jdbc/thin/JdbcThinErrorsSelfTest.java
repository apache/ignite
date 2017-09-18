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

import java.net.URL;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import org.apache.ignite.jdbc.JdbcErrorsAbstractSelfTest;
import org.apache.ignite.lang.IgniteCallable;

/**
 * Test SQLSTATE codes propagation with thin client driver.
 */
public class JdbcThinErrorsSelfTest extends JdbcErrorsAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected Connection getConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");
    }

    /**
     * Test error code for the case when connection string is fine but client can't reach server
     * due to <b>communication problems</b> (not due to clear misconfiguration).
     * @throws SQLException if failed.
     */
    public void testConnectionError() throws SQLException {
        checkErrorState(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                DriverManager.getConnection("jdbc:ignite:thin://unknown.host");

                return null;
            }
        }, "08001");
    }

    /**
     * Test error code for the case when connection string is a mess.
     * @throws SQLException if failed.
     */
    public void testInvalidConnectionStringFormat() throws SQLException {
        checkErrorState(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                // Invalid port number yields an error.
                DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:1000000");

                return null;
            }
        }, "08001");
    }

    /* ALL TESTS PAST THIS POINT MUST BE MOVED TO PARENT CLASS JdbcErrorsAbstractSelfTest
     * ONCE ERROR CODES RELATED WORK ON JDBC2 DRIVER IS FINISHED */

    /**
     * Test error code for the case when user attempts to use a closed connection.
     * @throws SQLException if failed.
     */
    public void testConnectionClosed() throws SQLException {
        checkErrorState(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                Connection conn = getConnection();

                conn.close();

                conn.prepareStatement("SELECT 1");

                return null;
            }
        }, "08003");

        checkErrorState(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                Connection conn = getConnection();

                conn.close();

                conn.createStatement();

                return null;
            }
        }, "08003");

        checkErrorState(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                Connection conn = getConnection();

                conn.close();

                conn.getMetaData();

                return null;
            }
        }, "08003");

        checkErrorState(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                Connection conn = getConnection();

                DatabaseMetaData meta = conn.getMetaData();

                conn.close();

                meta.getIndexInfo(null, null, null, false, false);

                return null;
            }
        }, "08003");

        checkErrorState(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                Connection conn = getConnection();

                DatabaseMetaData meta = conn.getMetaData();

                conn.close();

                meta.getColumns(null, null, null, null);

                return null;
            }
        }, "08003");

        checkErrorState(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                Connection conn = getConnection();

                DatabaseMetaData meta = conn.getMetaData();

                conn.close();

                meta.getPrimaryKeys(null, null, null);

                return null;
            }
        }, "08003");

        checkErrorState(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                Connection conn = getConnection();

                DatabaseMetaData meta = conn.getMetaData();

                conn.close();

                meta.getSchemas(null, null);

                return null;
            }
        }, "08003");

        checkErrorState(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                Connection conn = getConnection();

                DatabaseMetaData meta = conn.getMetaData();

                conn.close();

                meta.getTables(null, null, null, null);

                return null;
            }
        }, "08003");
    }

    /**
     * Test error code for the case when user attempts to use a closed result set.
     * @throws SQLException if failed.
     */
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
        }, "24000");
    }

    /**
     * Test error code for the case when user attempts to set an invalid isolation level to a connection.
     * @throws SQLException if failed.
     */
    @SuppressWarnings("MagicConstant")
    public void testInvalidIsolationLevel() throws SQLException {
        checkErrorState(new ConnClosure() {
            @Override public void run(Connection conn) throws Exception {
                conn.setTransactionIsolation(1000);
            }
        }, "0700E");
    }

    /**
     * Test error code for the case when user attempts to get {@code int} value
     * from column whose value can't be converted to an {@code int}.
     * @throws SQLException if failed.
     */
    public void testInvalidIntFormat() throws SQLException {
        checkErrorState(new ConnClosure() {
            @Override public void run(Connection conn) throws Exception {
                try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                    ResultSet rs = stmt.executeQuery();

                    rs.next();

                    rs.getLong(1);
                }
            }
        }, "0700B");
    }

    /**
     * Test error code for the case when user attempts to get {@code long} value
     * from column whose value can't be converted to an {@code long}.
     * @throws SQLException if failed.
     */
    public void testInvalidLongFormat() throws SQLException {
        checkErrorState(new ConnClosure() {
            @Override public void run(Connection conn) throws Exception {
                try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                    ResultSet rs = stmt.executeQuery();

                    rs.next();

                    rs.getLong(1);
                }
            }
        }, "0700B");
    }

    /**
     * Test error code for the case when user attempts to get {@code float} value
     * from column whose value can't be converted to an {@code float}.
     * @throws SQLException if failed.
     */
    public void testInvalidFloatFormat() throws SQLException {
        checkErrorState(new ConnClosure() {
            @Override public void run(Connection conn) throws Exception {
                try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                    ResultSet rs = stmt.executeQuery();

                    rs.next();

                    rs.getFloat(1);
                }
            }
        }, "0700B");
    }

    /**
     * Test error code for the case when user attempts to get {@code double} value
     * from column whose value can't be converted to an {@code double}.
     * @throws SQLException if failed.
     */
    public void testInvalidDoubleFormat() throws SQLException {
        checkErrorState(new ConnClosure() {
            @Override public void run(Connection conn) throws Exception {
                try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                    ResultSet rs = stmt.executeQuery();

                    rs.next();

                    rs.getDouble(1);
                }
            }
        }, "0700B");
    }

    /**
     * Test error code for the case when user attempts to get {@code byte} value
     * from column whose value can't be converted to an {@code byte}.
     * @throws SQLException if failed.
     */
    public void testInvalidByteFormat() throws SQLException {
        checkErrorState(new ConnClosure() {
            @Override public void run(Connection conn) throws Exception {
                try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                    ResultSet rs = stmt.executeQuery();

                    rs.next();

                    rs.getByte(1);
                }
            }
        }, "0700B");
    }

    /**
     * Test error code for the case when user attempts to get {@code short} value
     * from column whose value can't be converted to an {@code short}.
     * @throws SQLException if failed.
     */
    public void testInvalidShortFormat() throws SQLException {
        checkErrorState(new ConnClosure() {
            @Override public void run(Connection conn) throws Exception {
                try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                    ResultSet rs = stmt.executeQuery();

                    rs.next();

                    rs.getShort(1);
                }
            }
        }, "0700B");
    }

    /**
     * Test error code for the case when user attempts to get {@code BigDecimal} value
     * from column whose value can't be converted to an {@code BigDecimal}.
     * @throws SQLException if failed.
     */
    public void testInvalidBigDecimalFormat() throws SQLException {
        checkErrorState(new ConnClosure() {
            @Override public void run(Connection conn) throws Exception {
                try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                    ResultSet rs = stmt.executeQuery();

                    rs.next();

                    rs.getBigDecimal(1);
                }
            }
        }, "0700B");
    }

    /**
     * Test error code for the case when user attempts to get {@code boolean} value
     * from column whose value can't be converted to an {@code boolean}.
     * @throws SQLException if failed.
     */
    public void testInvalidBooleanFormat() throws SQLException {
        checkErrorState(new ConnClosure() {
            @Override public void run(Connection conn) throws Exception {
                try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                    ResultSet rs = stmt.executeQuery();

                    rs.next();

                    rs.getBoolean(1);
                }
            }
        }, "0700B");
    }

    /**
     * Test error code for the case when user attempts to get {@code boolean} value
     * from column whose value can't be converted to an {@code boolean}.
     * @throws SQLException if failed.
     */
    public void testInvalidObjectFormat() throws SQLException {
        checkErrorState(new ConnClosure() {
            @Override public void run(Connection conn) throws Exception {
                try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                    ResultSet rs = stmt.executeQuery();

                    rs.next();

                    rs.getObject(1, List.class);
                }
            }
        }, "0700B");
    }

    /**
     * Test error code for the case when user attempts to get {@link Date} value
     * from column whose value can't be converted to a {@link Date}.
     * @throws SQLException if failed.
     */
    public void testInvalidDateFormat() throws SQLException {
        checkErrorState(new ConnClosure() {
            @Override public void run(Connection conn) throws Exception {
                try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                    ResultSet rs = stmt.executeQuery();

                    rs.next();

                    rs.getDate(1);
                }
            }
        }, "0700B");
    }

    /**
     * Test error code for the case when user attempts to get {@link Time} value
     * from column whose value can't be converted to a {@link Time}.
     * @throws SQLException if failed.
     */
    public void testInvalidTimeFormat() throws SQLException {
        checkErrorState(new ConnClosure() {
            @Override public void run(Connection conn) throws Exception {
                try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                    ResultSet rs = stmt.executeQuery();

                    rs.next();

                    rs.getTime(1);
                }
            }
        }, "0700B");
    }

    /**
     * Test error code for the case when user attempts to get {@link Timestamp} value
     * from column whose value can't be converted to a {@link Timestamp}.
     * @throws SQLException if failed.
     */
    public void testInvalidTimestampFormat() throws SQLException {
        checkErrorState(new ConnClosure() {
            @Override public void run(Connection conn) throws Exception {
                try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                    ResultSet rs = stmt.executeQuery();

                    rs.next();

                    rs.getTimestamp(1);
                }
            }
        }, "0700B");
    }

    /**
     * Test error code for the case when user attempts to get {@link URL} value
     * from column whose value can't be converted to a {@link URL}.
     * @throws SQLException if failed.
     */
    public void testInvalidUrlFormat() throws SQLException {
        checkErrorState(new ConnClosure() {
            @Override public void run(Connection conn) throws Exception {
                try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                    ResultSet rs = stmt.executeQuery();

                    rs.next();

                    rs.getURL(1);
                }
            }
        }, "0700B");
    }

    /**
     * Check error code for the case null value is inserted into table field declared as NOT NULL.
     *
     * @throws SQLException if failed.
     */
    public void testNotNullViolation() throws SQLException {
        try (Connection conn = getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE nulltest(id INT PRIMARY KEY, name CHAR NOT NULL)");

                try {
                    checkErrorState(new IgniteCallable<Void>() {
                        @Override public Void call() throws Exception {
                            stmt.execute("INSERT INTO nulltest(id, name) VALUES (1, NULLIF('a', 'a'))");

                            return null;
                        }
                    }, "22004");
                }
                finally {
                    stmt.execute("DROP TABLE nulltest");
                }
            }
        }
    }
}
