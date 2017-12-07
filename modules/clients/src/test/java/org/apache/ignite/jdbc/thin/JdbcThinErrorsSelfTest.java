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

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
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
     * Test error code for the case when error is caused on batch execution.
     * @throws SQLException if failed.
     */
    @SuppressWarnings("MagicConstant")
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
                assertEquals(2, e.getUpdateCounts().length);

                for (int updCnt : e.getUpdateCounts())
                    assertEquals(1, updCnt);

                assertEquals("42000", e.getSQLState());
            }
        }
    }
}
