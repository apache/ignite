/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.jdbc.thin;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.ignite.jdbc.JdbcErrorsAbstractSelfTest;
import org.apache.ignite.lang.IgniteCallable;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

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
    @Test
    public void testConnectionError() throws SQLException {
        checkErrorState(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                DriverManager.getConnection("jdbc:ignite:thin://unknown.host");

                return null;
            }
        }, "08001", "Failed to connect to server [host=unknown.host");
    }

    /**
     * Test error code for the case when connection string is a mess.
     * @throws SQLException if failed.
     */
    @Test
    public void testInvalidConnectionStringFormat() throws SQLException {
        checkErrorState(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                // Invalid port number yields an error.
                DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:1000000");

                return null;
            }
        }, "08001", "port range contains invalid port 1000000");
    }

    /**
     * Test error code for the case when user attempts to set an invalid isolation level to a connection.
     * @throws SQLException if failed.
     */
    @SuppressWarnings("MagicConstant")
    @Test
    public void testInvalidIsolationLevel() throws SQLException {
        checkErrorState(new ConnClosure() {
            @Override public void run(Connection conn) throws Exception {
                conn.setTransactionIsolation(1000);
            }
        }, "0700E", "Invalid transaction isolation level.");
    }

    /**
     * Test error code for the case when error is caused on batch execution.
     * @throws SQLException if failed.
     */
    @Test
    public void testBatchUpdateException() throws SQLException {
        try (final Connection conn = getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("CREATE TABLE test (id int primary key, val varchar)");

                stmt.addBatch("insert into test (id, val) values (1, 'val1')");
                stmt.addBatch("insert into test (id, val) values (2, 'val2')");
                stmt.addBatch("insert into test (id1, val1) values (3, 'val3')");

                stmt.executeBatch();

                fail("BatchUpdateException is expected");
            }
            catch (BatchUpdateException e) {
                assertEquals(3, e.getUpdateCounts().length);

                assertArrayEquals("", new int[] {1, 1, Statement.EXECUTE_FAILED}, e.getUpdateCounts());

                assertEquals("42000", e.getSQLState());

                assertTrue("Unexpected error message: " + e.getMessage(), e.getMessage() != null &&
                    e.getMessage().contains("Failed to parse query. Column \"ID1\" not found"));
            }
        }
    }

    /**
     * Check that unsupported explain of update operation causes Exception on the driver side with correct code and
     * message.
     */
    @Test
    public void testExplainUpdatesUnsupported() throws Exception{
        checkErrorState((conn) -> {
            try (Statement statement = conn.createStatement()) {
                statement.executeUpdate("CREATE TABLE TEST_EXPLAIN (ID LONG PRIMARY KEY, VAL LONG)");

                statement.executeUpdate("EXPLAIN INSERT INTO TEST_EXPLAIN VALUES (1, 2)");
            }
        }, "0A000", "Explains of update queries are not supported.");
    }
}
