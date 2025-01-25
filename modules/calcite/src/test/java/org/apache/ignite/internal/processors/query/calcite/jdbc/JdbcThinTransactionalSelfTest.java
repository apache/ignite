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

package org.apache.ignite.internal.processors.query.calcite.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.IgniteConfigVariationsAbstractTest.TestRunnable;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.junit.Test;

import static java.sql.Connection.TRANSACTION_NONE;
import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_READ_UNCOMMITTED;
import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;
import static java.sql.Connection.TRANSACTION_SERIALIZABLE;
import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/** */
public class JdbcThinTransactionalSelfTest extends GridCommonAbstractTest {
    /** URL. */
    public static final String URL = "jdbc:ignite:thin://127.0.0.1";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getTransactionConfiguration().setTxAwareQueriesEnabled(true);
        cfg.getSqlConfiguration().setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid();
    }

    /** */
    @Test
    public void testDatabaseMetadata() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            assertTrue(meta.supportsTransactions());
            assertEquals(TRANSACTION_READ_COMMITTED, meta.getDefaultTransactionIsolation());
            assertTrue(meta.supportsDataManipulationTransactionsOnly());

            assertTrue(meta.supportsTransactionIsolationLevel(TRANSACTION_NONE));
            assertTrue(meta.supportsTransactionIsolationLevel(TRANSACTION_READ_COMMITTED));
            assertFalse(meta.supportsTransactionIsolationLevel(TRANSACTION_REPEATABLE_READ));
            assertFalse(meta.supportsTransactionIsolationLevel(TRANSACTION_SERIALIZABLE));
            assertFalse(meta.supportsTransactionIsolationLevel(TRANSACTION_READ_UNCOMMITTED));
        }
    }

    /** */
    @Test
    public void testInvalidHoldability() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            List<TestRunnable> checks = Arrays.asList(
                () -> conn.setHoldability(HOLD_CURSORS_OVER_COMMIT),
                () -> conn.createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT),
                () -> conn.prepareStatement("SELECT * FROM T", TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT)
            );

            assertEquals(CLOSE_CURSORS_AT_COMMIT, conn.getHoldability());

            for (TestRunnable check : checks) {
                assertThrows(
                    null,
                    () -> {
                        check.run();
                        return null;
                    },
                    SQLException.class,
                    "Invalid holdability (can't hold cursor over commit)."
                );
            }
        }
    }

    /** */
    @Test
    public void testTransactionConcurrencyProperty() throws Exception {
        for (TransactionConcurrency txConcurrency : TransactionConcurrency.values()) {
            String url = URL + "?transactionConcurrency=" + txConcurrency;

            try (Connection conn = DriverManager.getConnection(url)) {
                conn.setAutoCommit(false);

                try (ResultSet rs = conn.prepareStatement("SELECT 1").executeQuery()) {
                    assertEquals(1, F.size(grid().context().cache().context().tm().activeTransactions()));
                    assertEquals(txConcurrency, F.first(grid().context().cache().context().tm().activeTransactions()).concurrency());
                }
            }
        }
    }

    /** */
    @Test
    public void testTransactionIsolation() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            assertEquals(TRANSACTION_READ_COMMITTED, conn.getTransactionIsolation());

            conn.setTransactionIsolation(TRANSACTION_NONE);

            assertEquals(TRANSACTION_NONE, conn.getTransactionIsolation());

            for (int invalidIsolation : new int[]{TRANSACTION_READ_UNCOMMITTED, TRANSACTION_REPEATABLE_READ, TRANSACTION_SERIALIZABLE}) {
                assertThrows(
                    null,
                    () -> {
                        conn.setTransactionIsolation(invalidIsolation);
                        return null;
                    },
                    SQLException.class,
                    "Requested isolation level not supported by the server: " + invalidIsolation
                );
            }
        }
    }

    /** */
    @Test
    public void testChangeStreamInsideTransactionThrows() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            conn.setAutoCommit(false);

            conn.prepareStatement("SELECT 1").executeQuery();

            assertThrows(
                null,
                () -> {
                    conn.prepareStatement("SET STREAMING ON").executeUpdate();
                    return null;
                },
                SQLException.class,
                "Can't change stream mode inside transaction"
            );
        }
    }

    /** */
    @Test
    public void testNoTxInNoTxIsolation() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            conn.setTransactionIsolation(TRANSACTION_NONE);

            conn.prepareStatement("SELECT 1").executeQuery();

            assertNull(GridTestUtils.getFieldValue(conn, "txCtx"));
        }
    }

    /** */
    @Test
    public void testTransactionLabel() throws Exception {
        String url = URL + "?transactionLabel=mylabel";

        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(false);

            try (ResultSet rs = conn.prepareStatement("SELECT 1").executeQuery()) {
                assertEquals(1, F.size(grid().context().cache().context().tm().activeTransactions()));
                assertEquals("mylabel", F.first(grid().context().cache().context().tm().activeTransactions()).label());
            }
        }
    }

    /** */
    @Test
    public void testTransactionTimeout() throws Exception {
        int timeout = 1000;

        String url = URL + "?transactionTimeout=" + timeout;

        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(false);

            ResultSet rs = conn.prepareStatement("SELECT 1").executeQuery();

            Thread.sleep(3 * timeout);

            assertThrows(
                null,
                () -> {
                    rs.close();
                    conn.commit();
                    return null;
                },
                SQLException.class,
                "Cache transaction timed out"
            );
        }
    }

    /** */
    @Test
    public void testStatementsClosedOnTxEnd() throws Exception {
        for (boolean commit : new boolean[]{true, false}) {

            try (Connection conn = DriverManager.getConnection(URL)) {
                conn.setAutoCommit(false);

                PreparedStatement stmt0 = conn.prepareStatement("SELECT 1");
                PreparedStatement stmt1 = conn.prepareStatement("SELECT 1");

                ResultSet rs0 = stmt0.executeQuery();
                ResultSet rs1 = stmt1.executeQuery();

                assertFalse(stmt0.isClosed());
                assertFalse(stmt1.isClosed());
                assertFalse(rs0.isClosed());
                assertFalse(rs1.isClosed());

                if (commit)
                    conn.commit();
                else
                    conn.rollback();

                assertFalse(stmt0.isClosed());
                assertFalse(stmt1.isClosed());
                assertTrue(rs0.isClosed());
                assertTrue(rs1.isClosed());

                stmt0.close();
                assertTrue(stmt0.isClosed());
                assertFalse(stmt1.isClosed());

                assertNull(GridTestUtils.getFieldValue(conn, "txCtx"));
            }
        }
    }

    /** */
    @Test
    public void testCloseConnectionWithoutCommit() throws Exception {
        PreparedStatement stmt0;
        PreparedStatement stmt1;
        ResultSet rs0;
        ResultSet rs1;

        try (Connection conn = DriverManager.getConnection(URL)) {
            conn.setAutoCommit(false);

            stmt0 = conn.prepareStatement("SELECT 1");
            stmt1 = conn.prepareStatement("SELECT 1");

            rs0 = stmt0.executeQuery();
            rs1 = stmt1.executeQuery();

            assertFalse(stmt0.isClosed());
            assertFalse(stmt1.isClosed());
            assertFalse(rs0.isClosed());
            assertFalse(rs1.isClosed());
        }

        assertTrue(stmt0.isClosed());
        assertTrue(stmt1.isClosed());
        assertTrue(rs0.isClosed());
        assertTrue(rs1.isClosed());
    }

    /** */
    @Test
    public void testCreateStatementOnDefaults() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            conn.setAutoCommit(false);

            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT 1")) {
                    assertEquals(1, F.size(grid().context().cache().context().tm().activeTransactions()));

                    try (Statement stmt2 = conn.createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY)) {
                        try (ResultSet rs2 = stmt.executeQuery("SELECT 1")) {
                            assertEquals(1, F.size(grid().context().cache().context().tm().activeTransactions()));
                        }
                    }
                }
            }

        }
    }
}
