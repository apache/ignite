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
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.query.NestedTxMode;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/**
 * Tests to check behavior with transactions on.
 */
public class JdbcThinTransactionsSelfTest extends JdbcThinAbstractSelfTest {
    /** */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1";

    /** Logger. */
    private GridStringLogger log;

    /** {@inheritDoc} */
    @SuppressWarnings({"deprecation", "unchecked"})
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration(DEFAULT_CACHE_NAME).setNearConfiguration(null));

        cfg.setMarshaller(new BinaryMarshaller());

        cfg.setGridLogger(log = new GridStringLogger());

        return cfg;
    }

    /**
     * @param name Cache name.
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    private CacheConfiguration cacheConfiguration(@NotNull String name) throws Exception {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setName(name);
        cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrid(0);

        try (Connection c = c(true, NestedTxMode.ERROR)) {
            try (Statement s = c.createStatement()) {
                s.execute("CREATE TABLE INTS (k int primary key, v int) WITH \"cache_name=ints,wrap_value=false," +
                    "atomicity=transactional_snapshot\"");
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @param autoCommit Auto commit mode.
     * @param nestedTxMode Nested transactions mode.
     * @return Connection.
     * @throws SQLException if failed.
     */
    private static Connection c(boolean autoCommit, NestedTxMode nestedTxMode) throws SQLException {
        Connection res = DriverManager.getConnection(URL + "/?nestedTransactionsMode=" + nestedTxMode.name());

        res.setAutoCommit(autoCommit);

        return res;
    }

    /**
     *
     */
    @Test
    public void testTransactionsBeginCommitRollback() throws IgniteCheckedException {
        final AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                try {
                    try (Connection c = c(false, NestedTxMode.ERROR)) {
                        while (!stop.get()) {
                            try (Statement s = c.createStatement()) {
                                s.execute("BEGIN");

                                c.commit();

                                s.execute("BEGIN");

                                c.rollback();
                            }
                        }
                    }
                }
                catch (SQLException e) {
                    throw new AssertionError(e);
                }
            }
        }, 8, "jdbc-transactions");

        U.sleep(5000);

        stop.set(true);

        fut.get();
    }

    /**
     *
     */
    @Test
    public void testTransactionsBeginCommitRollbackAutocommit() throws IgniteCheckedException {
        GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                try {
                    try (Connection c = c(true, NestedTxMode.ERROR)) {
                        try (Statement s = c.createStatement()) {
                            s.execute("BEGIN");

                            s.execute("COMMIT");

                            s.execute("BEGIN");

                            s.execute("ROLLBACK");
                        }
                    }
                }
                catch (SQLException e) {
                    throw new AssertionError(e);
                }
            }
        }, 8, "jdbc-transactions").get();
    }

    /**
     *
     */
    @Test
    public void testIgnoreNestedTxAutocommitOff() throws SQLException {
        try (Connection c = c(false, NestedTxMode.IGNORE)) {
            doNestedTxStart(c, false);
        }

        assertTrue(log.toString().contains("ignoring BEGIN command"));
    }

    /**
     *
     */
    @Test
    public void testCommitNestedTxAutocommitOff() throws SQLException {
        try (Connection c = c(false, NestedTxMode.COMMIT)) {
            doNestedTxStart(c, false);
        }

        assertFalse(log.toString().contains("ignoring BEGIN command"));
    }

    /**
     *
     */
    @Test
    public void testErrorNestedTxAutocommitOff() throws SQLException {
        GridTestUtils.assertThrows(null, new Callable<Void>() {
            @Override public Void call() throws Exception {
                try (Connection c = c(false, NestedTxMode.ERROR)) {
                    doNestedTxStart(c, false);
                }

                throw new AssertionError();
            }
        }, SQLException.class, "Transaction has already been started.");
    }

    /**
     *
     */
    @Test
    public void testIgnoreNestedTxAutocommitOn() throws SQLException {
        try (Connection c = c(true, NestedTxMode.IGNORE)) {
            doNestedTxStart(c, false);
        }

        assertTrue(log.toString().contains("ignoring BEGIN command"));
    }

    /**
     *
     */
    @Test
    public void testCommitNestedTxAutocommitOn() throws SQLException {
        try (Connection c = c(true, NestedTxMode.COMMIT)) {
            doNestedTxStart(c, false);
        }

        assertFalse(log.toString().contains("ignoring BEGIN command"));
    }

    /**
     *
     */
    @Test
    public void testErrorNestedTxAutocommitOn() throws SQLException {
        GridTestUtils.assertThrows(null, new Callable<Void>() {
            @Override public Void call() throws Exception {
                try (Connection c = c(true, NestedTxMode.ERROR)) {
                    doNestedTxStart(c, false);
                }

                throw new AssertionError();
            }
        }, SQLException.class, "Transaction has already been started.");
    }

    /**
     *
     */
    @Test
    public void testIgnoreNestedTxAutocommitOffBatched() throws SQLException {
        try (Connection c = c(false, NestedTxMode.IGNORE)) {
            doNestedTxStart(c, true);
        }

        assertTrue(log.toString().contains("ignoring BEGIN command"));
    }

    /**
     *
     */
    @Test
    public void testCommitNestedTxAutocommitOffBatched() throws SQLException {
        try (Connection c = c(false, NestedTxMode.COMMIT)) {
            doNestedTxStart(c, true);
        }

        assertFalse(log.toString().contains("ignoring BEGIN command"));
    }

    /**
     *
     */
    @Test
    public void testErrorNestedTxAutocommitOffBatched() throws SQLException {
        GridTestUtils.assertThrows(null, new Callable<Void>() {
            @Override public Void call() throws Exception {
                try (Connection c = c(false, NestedTxMode.ERROR)) {
                    doNestedTxStart(c, true);
                }

                throw new AssertionError();
            }
        }, BatchUpdateException.class, "Transaction has already been started.");
    }

    /**
     *
     */
    @Test
    public void testIgnoreNestedTxAutocommitOnBatched() throws SQLException {
        try (Connection c = c(true, NestedTxMode.IGNORE)) {
            doNestedTxStart(c, true);
        }

        assertTrue(log.toString().contains("ignoring BEGIN command"));
    }

    /**
     *
     */
    @Test
    public void testCommitNestedTxAutocommitOnBatched() throws SQLException {
        try (Connection c = c(true, NestedTxMode.COMMIT)) {
            doNestedTxStart(c, true);
        }

        assertFalse(log.toString().contains("ignoring BEGIN command"));
    }

    /**
     *
     */
    @Test
    public void testErrorNestedTxAutocommitOnBatched() throws SQLException {
        GridTestUtils.assertThrows(null, new Callable<Void>() {
            @Override public Void call() throws Exception {
                try (Connection c = c(true, NestedTxMode.ERROR)) {
                    doNestedTxStart(c, true);
                }

                throw new AssertionError();
            }
        }, BatchUpdateException.class, "Transaction has already been started.");
    }

    /**
     * Try to start nested transaction via batch as well as separate statements.
     * @param conn Connection.
     * @param batched Whether {@link Statement#executeBatch()} should be used.
     * @throws SQLException if failed.
     */
    private void doNestedTxStart(Connection conn, boolean batched) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.executeQuery("SELECT * FROM INTS");

            if (batched) {
                s.addBatch("BEGIN");

                s.addBatch("BEGIN");

                s.executeBatch();
            }
            else {
                s.execute("BEGIN");

                s.execute("BEGIN");
            }
        }
    }

    /**
     * @throws SQLException if failed.
     */
    @Test
    public void testAutoCommitSingle() throws SQLException {
        doTestAutoCommit(false);
    }

    /**
     * @throws SQLException if failed.
     */
    @Test
    public void testAutoCommitBatched() throws SQLException {
        doTestAutoCommit(true);
    }

    /**
     * @param batched Batch mode flag.
     * @throws SQLException if failed.
     */
    private void doTestAutoCommit(boolean batched) throws SQLException {
        IgniteCache<Integer, ?> cache = grid(0).cache("ints");

        try (Connection c = c(false, NestedTxMode.ERROR)) {
            try (Statement s = c.createStatement()) {
                assertFalse(s.executeQuery("SELECT * from INTS").next());

                if (batched) {
                    s.addBatch("INSERT INTO INTS(k, v) values(1, 1)");

                    s.executeBatch();
                }
                else
                    s.execute("INSERT INTO INTS(k, v) values(1, 1)");

                // We haven't committed anything yet - this check shows that autoCommit flag is in effect.
                assertTrue(cache.query(new SqlFieldsQuery("SELECT * from INTS")).getAll().isEmpty());

                // We should see own updates.
                assertTrue(s.executeQuery("SELECT * from INTS").next());

                c.commit();

                c.setAutoCommit(true);

                assertEquals(1, cache.get(1));

                assertTrue(s.executeQuery("SELECT * from INTS").next());
            }
        }
    }

    /**
     * Test that exception in one of the statements does not kill connection worker altogether.
     * @throws SQLException if failed.
     */
    @Test
    public void testExceptionHandling() throws SQLException {
        try (Connection c = c(true, NestedTxMode.ERROR)) {
            try (Statement s = c.createStatement()) {
                s.execute("INSERT INTO INTS(k, v) values(1, 1)");

                assertEquals(1, grid(0).cache("ints").get(1));

                GridTestUtils.assertThrows(null, new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        s.execute("INSERT INTO INTS(x, y) values(1, 1)");

                        return null;
                    }
                }, SQLException.class, "Failed to parse query");

                s.execute("INSERT INTO INTS(k, v) values(2, 2)");

                assertEquals(2, grid(0).cache("ints").get(2));
            }
        }
    }

    /**
     * Test that exception in one of the statements does not kill connection worker altogether.
     * @throws SQLException if failed.
     */
    @Test
    public void testParsingErrorHasNoSideEffect() throws SQLException {
        try (Connection c = c(false, NestedTxMode.ERROR)) {
            try (Statement s = c.createStatement()) {
                s.execute("INSERT INTO INTS(k, v) values(1, 1)");

                GridTestUtils.assertThrows(null, new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        s.execute("INSERT INTO INTS(k, v) values(1)");

                        return null;
                    }
                }, SQLException.class, "Failed to parse query");

                s.execute("INSERT INTO INTS(k, v) values(2, 2)");

                c.commit();
            }

            assertEquals(1, grid(0).cache("ints").get(1));
            assertEquals(2, grid(0).cache("ints").get(2));
        }
    }
}
