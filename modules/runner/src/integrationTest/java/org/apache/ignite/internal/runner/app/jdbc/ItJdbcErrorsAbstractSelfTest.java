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

import static org.apache.ignite.client.proto.query.SqlStateCode.CONNECTION_CLOSED;
import static org.apache.ignite.client.proto.query.SqlStateCode.CONVERSION_FAILED;
import static org.apache.ignite.client.proto.query.SqlStateCode.INVALID_CURSOR_STATE;
import static org.apache.ignite.client.proto.query.SqlStateCode.NULL_VALUE;
import static org.apache.ignite.client.proto.query.SqlStateCode.PARSING_EXCEPTION;
import static org.apache.ignite.client.proto.query.SqlStateCode.UNSUPPORTED_OPERATION;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URL;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Test SQLSTATE codes propagation with (any) Ignite JDBC driver.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-15655")
public abstract class ItJdbcErrorsAbstractSelfTest extends AbstractJdbcSelfTest {
    /**
     * Test that parsing-related error codes get propagated to Ignite SQL exceptions.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15247")
    public void testParsingErrors() {
        checkErrorState("gibberish", PARSING_EXCEPTION,
                "Failed to parse query. Syntax error in SQL statement \"GIBBERISH[*] \"");
    }

    /**
     * Test that error codes from tables related DDL operations get propagated to Ignite SQL
     * exceptions.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15247")
    public void testTableErrors() {
        checkErrorState("DROP TABLE \"PUBLIC\".missing", PARSING_EXCEPTION, "Table doesn't exist: MISSING");
    }

    /**
     * Test that error codes from indexes related DDL operations get propagated to Ignite SQL
     * exceptions.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15247")
    public void testIndexErrors() {
        checkErrorState("DROP INDEX \"PUBLIC\".missing", PARSING_EXCEPTION, "Index doesn't exist: MISSING");
    }

    /**
     * Test that error codes from DML operations get propagated to Ignite SQL exceptions.
     *
     * @throws SQLException if failed.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15247")
    public void testDmlErrors() throws SQLException {
        stmt.execute("CREATE TABLE INTEGER(KEY INT PRIMARY KEY, VAL INT)");

        try {
            checkErrorState("INSERT INTO INTEGER(key, val) values(1, null)", NULL_VALUE,
                    "Value for INSERT, COPY, MERGE, or UPDATE must not be null");

            checkErrorState("INSERT INTO INTEGER(key, val) values(1, 'zzz')", CONVERSION_FAILED,
                    "Value conversion failed [column=_VAL, from=java.lang.String, to=java.lang.Integer]");
        } finally {
            stmt.execute("DROP TABLE INTEGER;");
        }
    }

    /**
     * Test error code for the case when user attempts to refer a future currently unsupported.
     *
     * @throws SQLException if failed.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15247")
    public void testUnsupportedSql() throws SQLException {
        stmt.execute("CREATE TABLE INTEGER(KEY INT PRIMARY KEY, VAL INT)");

        try {
            checkErrorState("ALTER TABLE INTEGER MODIFY COLUMN KEY CHAR", UNSUPPORTED_OPERATION,
                    "ALTER COLUMN is not supported");
        } finally {
            stmt.execute("DROP TABLE INTEGER;");
        }
    }

    /**
     * Test error code for the case when user attempts to use a closed connection.
     *
     * @throws SQLException if failed.
     */
    @Test
    public void testConnectionClosed() throws SQLException {
        Connection conn = getConnection();
        DatabaseMetaData meta = conn.getMetaData();

        conn.close();

        checkErrorState(() -> conn.prepareStatement("SELECT 1"), CONNECTION_CLOSED, "Connection is closed.");
        checkErrorState(() -> conn.createStatement(), CONNECTION_CLOSED, "Connection is closed.");
        checkErrorState(() -> conn.getMetaData(), CONNECTION_CLOSED, "Connection is closed.");
        checkErrorState(() -> meta.getIndexInfo(null, null, null, false, false), CONNECTION_CLOSED, "Connection is closed.");
        checkErrorState(() -> meta.getColumns(null, null, null, null), CONNECTION_CLOSED, "Connection is closed.");
        checkErrorState(() -> meta.getPrimaryKeys(null, null, null), CONNECTION_CLOSED, "Connection is closed.");
        checkErrorState(() -> meta.getSchemas(null, null), CONNECTION_CLOSED, "Connection is closed.");
        checkErrorState(() -> meta.getTables(null, null, null, null), CONNECTION_CLOSED, "Connection is closed.");
    }

    /**
     * Test error code for the case when user attempts to use a closed result set.
     */
    @Test
    public void testResultSetClosed() {
        checkErrorState(() -> {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT 1")) {
                ResultSet rs = stmt.executeQuery();

                rs.next();

                rs.close();

                rs.getInt(1);
            }
        }, INVALID_CURSOR_STATE, "Result set is closed");
    }

    /**
     * Test error code for the case when user attempts to get {@code int} value from column whose
     * value can't be converted to an {@code int}.
     */
    @Test
    public void testInvalidIntFormat() {
        checkErrorState(() -> {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                ResultSet rs = stmt.executeQuery();

                rs.next();

                rs.getInt(1);
            }
        }, CONVERSION_FAILED, "Cannot convert to int");
    }

    /**
     * Test error code for the case when user attempts to get {@code long} value from column whose
     * value can't be converted to an {@code long}.
     */
    @Test
    public void testInvalidLongFormat() {
        checkErrorState(() -> {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                ResultSet rs = stmt.executeQuery();

                rs.next();

                rs.getLong(1);
            }
        }, CONVERSION_FAILED, "Cannot convert to long");
    }

    /**
     * Test error code for the case when user attempts to get {@code float} value from column whose
     * value can't be converted to an {@code float}.
     */
    @Test
    public void testInvalidFloatFormat() {
        checkErrorState(() -> {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                ResultSet rs = stmt.executeQuery();

                rs.next();

                rs.getFloat(1);
            }
        }, CONVERSION_FAILED, "Cannot convert to float");
    }

    /**
     * Test error code for the case when user attempts to get {@code double} value from column whose
     * value can't be converted to an {@code double}.
     */
    @Test
    public void testInvalidDoubleFormat() {
        checkErrorState(() -> {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                ResultSet rs = stmt.executeQuery();

                rs.next();

                rs.getDouble(1);
            }
        }, CONVERSION_FAILED, "Cannot convert to double");
    }

    /**
     * Test error code for the case when user attempts to get {@code byte} value from column whose
     * value can't be converted to an {@code byte}.
     */
    @Test
    public void testInvalidByteFormat() {
        checkErrorState(() -> {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                ResultSet rs = stmt.executeQuery();

                rs.next();

                rs.getByte(1);
            }
        }, CONVERSION_FAILED, "Cannot convert to byte");
    }

    /**
     * Test error code for the case when user attempts to get {@code short} value from column whose
     * value can't be converted to an {@code short}.
     */
    @Test
    public void testInvalidShortFormat() {
        checkErrorState(() -> {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                ResultSet rs = stmt.executeQuery();

                rs.next();

                rs.getShort(1);
            }
        }, CONVERSION_FAILED, "Cannot convert to short");
    }

    /**
     * Test error code for the case when user attempts to get {@code BigDecimal} value from column
     * whose value can't be converted to an {@code BigDecimal}.
     */
    @Test
    public void testInvalidBigDecimalFormat() {
        checkErrorState(() -> {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                ResultSet rs = stmt.executeQuery();

                rs.next();

                rs.getBigDecimal(1);
            }
        }, CONVERSION_FAILED, "Cannot convert to BigDecimal");
    }

    /**
     * Test error code for the case when user attempts to get {@code boolean} value from column
     * whose value can't be converted to an {@code boolean}.
     */
    @Test
    public void testInvalidBooleanFormat() {
        checkErrorState(() -> {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                ResultSet rs = stmt.executeQuery();

                rs.next();

                rs.getBoolean(1);
            }
        }, CONVERSION_FAILED, "Cannot convert to boolean");
    }

    /**
     * Test error code for the case when user attempts to get {@code boolean} value from column
     * whose value can't be converted to an {@code boolean}.
     */
    @Test
    public void testInvalidObjectFormat() {
        checkErrorState(() -> {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                ResultSet rs = stmt.executeQuery();

                rs.next();

                rs.getObject(1, List.class);
            }
        }, CONVERSION_FAILED, "Cannot convert to java.util.List");
    }

    /**
     * Test error code for the case when user attempts to get {@link Date} value from column whose
     * value can't be converted to a {@link Date}.
     */
    @Test
    public void testInvalidDateFormat() {
        checkErrorState(() -> {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                ResultSet rs = stmt.executeQuery();

                rs.next();

                rs.getDate(1);
            }
        }, CONVERSION_FAILED, "Cannot convert to date");
    }

    /**
     * Test error code for the case when user attempts to get {@link Time} value from column whose
     * value can't be converted to a {@link Time}.
     */
    @Test
    public void testInvalidTimeFormat() {
        checkErrorState(() -> {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                ResultSet rs = stmt.executeQuery();

                rs.next();

                rs.getTime(1);
            }
        }, CONVERSION_FAILED, "Cannot convert to time");
    }

    /**
     * Test error code for the case when user attempts to get {@link Timestamp} value from column
     * whose value can't be converted to a {@link Timestamp}.
     */
    @Test
    public void testInvalidTimestampFormat() {
        checkErrorState(() -> {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                ResultSet rs = stmt.executeQuery();

                rs.next();

                rs.getTimestamp(1);
            }
        }, CONVERSION_FAILED, "Cannot convert to timestamp");
    }

    /**
     * Test error code for the case when user attempts to get {@link URL} value from column whose
     * value can't be converted to a {@link URL}.
     */
    @Test
    public void testInvalidUrlFormat() {
        checkErrorState(() -> {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT 'zzz'")) {
                ResultSet rs = stmt.executeQuery();

                rs.next();

                rs.getURL(1);
            }
        }, CONVERSION_FAILED, "Cannot convert to URL");
    }

    /**
     * Check error code for the case null value is inserted into table field declared as NOT NULL.
     *
     * @throws SQLException if failed.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15247")
    public void testNotNullViolation() throws SQLException {
        stmt.execute("CREATE TABLE public.nulltest(id INT PRIMARY KEY, name CHAR NOT NULL)");

        try {
            checkErrorState(() -> stmt.execute("INSERT INTO public.nulltest(id, name) VALUES (1, NULLIF('a', 'a'))"),
                    NULL_VALUE, "Null value is not allowed for column 'NAME'");
        } finally {
            stmt.execute("DROP TABLE public.nulltest");
        }
    }

    /**
     * Checks wrong table name select error message.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15247")
    public void testSelectWrongTable() {
        checkSqlErrorMessage("select from wrong", PARSING_EXCEPTION,
                "Failed to parse query. Table \"WRONG\" not found");
    }

    /**
     * Checks wrong column name select error message.
     *
     * @throws SQLException If failed.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15247")
    public void testSelectWrongColumnName() throws SQLException {
        stmt.execute("CREATE TABLE public.test(id INT PRIMARY KEY, name CHAR NOT NULL)");

        try {
            checkSqlErrorMessage("select wrong from public.test", PARSING_EXCEPTION,
                    "Failed to parse query. Column \"WRONG\" not found");
        } finally {
            stmt.execute("DROP TABLE public.test");
        }
    }

    /**
     * Checks wrong syntax select error message.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15247")
    public void testSelectWrongSyntax() {
        checkSqlErrorMessage("select from test where", PARSING_EXCEPTION,
                "Failed to parse query. Syntax error in SQL statement \"SELECT FROM TEST WHERE[*]");
    }

    /**
     * Checks wrong table name DML error message.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15247")
    public void testDmlWrongTable() {
        checkSqlErrorMessage("insert into wrong (id, val) values (3, 'val3')", PARSING_EXCEPTION,
                "Failed to parse query. Table \"WRONG\" not found");

        checkSqlErrorMessage("merge into wrong (id, val) values (3, 'val3')", PARSING_EXCEPTION,
                "Failed to parse query. Table \"WRONG\" not found");

        checkSqlErrorMessage("update wrong set val = 'val3' where id = 2", PARSING_EXCEPTION,
                "Failed to parse query. Table \"WRONG\" not found");

        checkSqlErrorMessage("delete from wrong where id = 2", PARSING_EXCEPTION,
                "Failed to parse query. Table \"WRONG\" not found");
    }

    /**
     * Checks wrong column name DML error message.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15247")
    public void testDmlWrongColumnName() {
        checkSqlErrorMessage("insert into test (id, wrong) values (3, 'val3')", PARSING_EXCEPTION,
                "Failed to parse query. Column \"WRONG\" not found");

        checkSqlErrorMessage("merge into test (id, wrong) values (3, 'val3')", PARSING_EXCEPTION,
                "Failed to parse query. Column \"WRONG\" not found");

        checkSqlErrorMessage("update test set wrong = 'val3' where id = 2", PARSING_EXCEPTION,
                "Failed to parse query. Column \"WRONG\" not found");

        checkSqlErrorMessage("delete from test where wrong = 2", PARSING_EXCEPTION,
                "Failed to parse query. Column \"WRONG\" not found");
    }

    /**
     * Checks wrong syntax DML error message.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15247")
    public void testDmlWrongSyntax() {
        checkSqlErrorMessage("insert test (id, val) values (3, 'val3')", PARSING_EXCEPTION,
                "Failed to parse query. Syntax error in SQL statement \"INSERT TEST[*] (ID, VAL)");

        checkSqlErrorMessage("merge test (id, val) values (3, 'val3')", PARSING_EXCEPTION,
                "Failed to parse query. Syntax error in SQL statement \"MERGE TEST[*] (ID, VAL)");

        checkSqlErrorMessage("update test val = 'val3' where id = 2", PARSING_EXCEPTION,
                "Failed to parse query. Syntax error in SQL statement \"UPDATE TEST VAL =[*] 'val3' WHERE ID = 2");

        checkSqlErrorMessage("delete from test 1where id = 2", PARSING_EXCEPTION,
                "Failed to parse query. Syntax error in SQL statement \"DELETE FROM TEST 1[*]WHERE ID = 2 ");
    }

    /**
     * Checks wrong table name DDL error message.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15247")
    public void testDdlWrongTable() {
        checkSqlErrorMessage("create table test (id int primary key, val varchar)", PARSING_EXCEPTION,
                "Table already exists: TEST");

        checkSqlErrorMessage("drop table wrong", PARSING_EXCEPTION,
                "Table doesn't exist: WRONG");

        checkSqlErrorMessage("create index idx1 on wrong (val)", PARSING_EXCEPTION,
                "Table doesn't exist: WRONG");

        checkSqlErrorMessage("drop index wrong", PARSING_EXCEPTION,
                "Index doesn't exist: WRONG");

        checkSqlErrorMessage("alter table wrong drop column val", PARSING_EXCEPTION,
                "Failed to parse query. Table \"WRONG\" not found");
    }

    /**
     * Checks wrong column name DDL error message.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15247")
    public void testDdlWrongColumnName() {
        checkSqlErrorMessage("alter table test drop column wrong", PARSING_EXCEPTION,
                "Failed to parse query. Column \"WRONG\" not found");

        checkSqlErrorMessage("create index idx1 on test (wrong)", PARSING_EXCEPTION,
                "Column doesn't exist: WRONG");

        checkSqlErrorMessage("create table test(id integer primary key, AgE integer, AGe integer)",
                PARSING_EXCEPTION,
                "Duplicate column name: AGE");

        checkSqlErrorMessage(
                "create table test(\"id\" integer primary key, \"age\" integer, \"age\" integer)",
                PARSING_EXCEPTION,
                "Duplicate column name: age");

        checkSqlErrorMessage("create table test(id integer primary key, age integer, age varchar)",
                PARSING_EXCEPTION,
                "Duplicate column name: AGE");
    }

    /**
     * Checks wrong syntax DDL error message.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15247")
    public void testDdlWrongSyntax() {
        checkSqlErrorMessage("create table test2 (id int wrong key, val varchar)", PARSING_EXCEPTION,
                "Failed to parse query. Syntax error in SQL statement \"CREATE TABLE TEST2 (ID INT WRONG[*]");

        checkSqlErrorMessage("drop table test on", PARSING_EXCEPTION,
                "Failed to parse query. Syntax error in SQL statement \"DROP TABLE TEST ON[*]");

        checkSqlErrorMessage("create index idx1 test (val)", PARSING_EXCEPTION,
                "Failed to parse query. Syntax error in SQL statement \"CREATE INDEX IDX1 TEST[*]");

        checkSqlErrorMessage("drop index", PARSING_EXCEPTION,
                "Failed to parse query. Syntax error in SQL statement \"DROP INDEX [*]");

        checkSqlErrorMessage("alter table test drop column", PARSING_EXCEPTION,
                "Failed to parse query. Syntax error in SQL statement \"ALTER TABLE TEST DROP COLUMN [*]");
    }

    /**
     * Gets the connection.
     *
     * @return Connection to execute statements on.
     * @throws SQLException if failed.
     */
    protected abstract Connection getConnection() throws SQLException;

    /**
     * Test that running given SQL statement yields expected SQLSTATE code.
     *
     * @param sql      Statement.
     * @param expState Expected SQLSTATE code.
     * @param expMsg   Expected message.
     */
    private void checkErrorState(final String sql, String expState, String expMsg) {
        checkErrorState(() -> {
            try (final PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.execute();
            }
        }, expState, expMsg);
    }

    /**
     * Test that running given closure yields expected SQLSTATE code.
     *
     * @param clo      Closure.
     * @param expState Expected SQLSTATE code.
     * @param expMsg   Expected message.
     */
    protected void checkErrorState(final RunnableX clo, String expState, String expMsg) {
        SQLException ex = assertThrows(SQLException.class, clo::run, expMsg);
        assertThat(ex.getMessage(), containsString(expMsg));

        assertEquals(expState, ex.getSQLState(), ex.getMessage());
    }

    /**
     * Check SQL exception message and error code.
     *
     * @param sql      Query string.
     * @param expState Error code.
     * @param expMsg   Error message.
     */
    private void checkSqlErrorMessage(final String sql, String expState, String expMsg) {
        checkErrorState(() -> {
            stmt.executeUpdate("DROP TABLE IF EXISTS wrong");
            stmt.executeUpdate("DROP TABLE IF EXISTS test");

            stmt.executeUpdate("CREATE TABLE test (id INT PRIMARY KEY, val VARCHAR)");

            stmt.executeUpdate("INSERT INTO test (id, val) VALUES (1, 'val1')");
            stmt.executeUpdate("INSERT INTO test (id, val) VALUES (2, 'val2')");

            stmt.execute(sql);

            fail("Exception is expected");
        }, expState, expMsg);
    }

    /**
     * A runnable that can throw an SQLException.
     */
    public interface RunnableX {
        /**
         * Runs this runnable.
         */
        void run() throws SQLException;
    }
}