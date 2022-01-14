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

import static org.apache.ignite.client.proto.query.SqlStateCode.CLIENT_CONNECTION_FAILED;
import static org.apache.ignite.client.proto.query.SqlStateCode.INVALID_TRANSACTION_LEVEL;
import static org.apache.ignite.client.proto.query.SqlStateCode.PARSING_EXCEPTION;
import static org.apache.ignite.client.proto.query.SqlStateCode.UNSUPPORTED_OPERATION;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Test SQLSTATE codes propagation with thin client driver.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-15655")
public class ItJdbcErrorsSelfTest extends ItJdbcErrorsAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected Connection getConnection() throws SQLException {
        return DriverManager.getConnection(URL);
    }

    /**
     * Test error code for the case when connection string is fine but client can't reach server
     * due to <b>communication problems</b> (not due to clear misconfiguration).
     */
    @Test
    public void testConnectionError() {
        checkErrorState(() -> DriverManager.getConnection("jdbc:ignite:thin://unknown.host?connectionTimeout=1000"),
                CLIENT_CONNECTION_FAILED, "Failed to connect to server");
    }

    /**
     * Test error code for the case when connection string is a mess.
     */
    @Test
    public void testInvalidConnectionStringFormat() {
        checkErrorState(() -> DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:1000000"),
                CLIENT_CONNECTION_FAILED, "port range contains invalid port 1000000");
    }

    /**
     * Test error code for the case when user attempts to set an invalid isolation level to a connection.
     */
    @SuppressWarnings("MagicConstant")
    @Test
    public void testInvalidIsolationLevel() {
        checkErrorState(() -> conn.setTransactionIsolation(1000),
                INVALID_TRANSACTION_LEVEL, "Invalid transaction isolation level.");
    }

    /**
     * Test error code for the case when error is caused on batch execution.
     *
     * @throws SQLException if failed.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-16201")
    public void testBatchUpdateException() throws SQLException {
        try {
            stmt.executeUpdate("CREATE TABLE test2 (id int primary key, val varchar)");

            stmt.addBatch("insert into test2 (id, val) values (1, 'val1')");
            stmt.addBatch("insert into test2 (id, val) values (2, 'val2')");
            stmt.addBatch("insert into test2 (id1, val1) values (3, 'val3')");

            stmt.executeBatch();

            fail("BatchUpdateException is expected");
        } catch (BatchUpdateException e) {
            assertEquals(3, e.getUpdateCounts().length);

            assertArrayEquals(new int[] {1, 1, Statement.EXECUTE_FAILED}, e.getUpdateCounts());

            assertEquals(PARSING_EXCEPTION, e.getSQLState());

            assertTrue(e.getMessage() != null
                            && e.getMessage().contains("Failed to parse query. Column \"ID1\" not found"),
                    "Unexpected error message: " + e.getMessage());
        }
    }

    /**
     * Check that unsupported explain of update operation causes Exception on the driver side with correct code and
     * message.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-15247")
    public void testExplainUpdatesUnsupported() {
        checkErrorState(() -> {
            stmt.executeUpdate("CREATE TABLE TEST_EXPLAIN (ID INT PRIMARY KEY, VAL INT)");

            stmt.executeUpdate("EXPLAIN INSERT INTO TEST_EXPLAIN VALUES (1, 2)");
        }, UNSUPPORTED_OPERATION, "Explains of update queries are not supported.");
    }
}
