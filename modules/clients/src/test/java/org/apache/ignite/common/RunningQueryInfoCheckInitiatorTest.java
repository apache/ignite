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

package org.apache.ignite.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.jdbc.thin.JdbcThinAbstractSelfTest;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.IgniteJdbcDriver.CFG_URL_PREFIX;

/**
 * Tests for query originator.
 */
public class RunningQueryInfoCheckInitiatorTest extends JdbcThinAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)))
            .setAuthenticationEnabled(true)
            .setCacheConfiguration(new CacheConfiguration()
                .setName("test")
                .setSqlSchema("TEST")
                .setSqlFunctionClasses(TestSQLFunctions.class)
                .setIndexedTypes(Integer.class, Integer.class)
            );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        startGrid(0);
        startClientGrid(1);

        grid(0).cluster().state(ClusterState.ACTIVE);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (String cache : grid(0).cacheNames()) {
            if (!cache.equals("test"))
                grid(0).cache(cache).destroy();
        }

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUserDefinedInitiatorId() throws Exception {
        final String initiatorId = "TestUserSpecifiedOriginator";

        Consumer<String> sqlExec = sql -> GridTestUtils.runAsync(() -> {
            try {
                grid(0).context().query().querySqlFields(
                    new SqlFieldsQuery(sql).setQueryInitiatorId(initiatorId), false).getAll();
            }
            catch (Exception e) {
                log.error("Unexpected exception", e);
                fail("Unexpected exception");
            }
        });

        Consumer<String> initiatorChecker = initId0 -> assertEquals(initiatorId, initId0);

        check(sqlExec, initiatorChecker);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleStatementsUserDefinedInitiatorId() throws Exception {
        final String initiatorId = "TestUserSpecifiedOriginator";

        GridTestUtils.runAsync(() -> {
            List<FieldsQueryCursor<List<?>>> curs = grid(0).context().query().querySqlFields(
                new SqlFieldsQuery("SELECT 'qry0', test.await(); SELECT 'qry1', test.await()")
                    .setQueryInitiatorId(initiatorId), false, false);

            for (FieldsQueryCursor<List<?>> cur : curs)
                cur.getAll();
        });

        assertEquals(initiatorId, initiatorId(grid(0), "qry0", 1000));

        TestSQLFunctions.unlockQuery();

        assertEquals(initiatorId, initiatorId(grid(0), "qry1", 1000));

        TestSQLFunctions.unlockQuery();

        checkRunningQueriesCount(grid(0), 0, 1000);
    }


    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJdbcThinInitiatorId() throws Exception {
        Consumer<String> sqlExec = sql -> {
            GridTestUtils.runAsync(() -> {
                    try (Connection conn = DriverManager.getConnection(
                        "jdbc:ignite:thin://127.0.0.1:" + clientPort(grid(0)) + "/?user=ignite&password=ignite")) {
                        try (Statement stmt = conn.createStatement()) {
                            stmt.execute(sql);
                        }
                    }
                    catch (SQLException e) {
                        log.error("Unexpected exception", e);
                    }
                }
            );
        };

        Consumer<String> initiatorChecker = initiatorId -> {
            assertTrue("Invalid initiator ID: " + initiatorId,
                Pattern.compile("jdbc-thin:127\\.0\\.0\\.1:[0-9]+@ignite").matcher(initiatorId).matches());
        };

        check(sqlExec, initiatorChecker);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testThinClientInitiatorId() throws Exception {
        Consumer<String> sqlExec = sql -> {
            GridTestUtils.runAsync(() -> {
                    try (IgniteClient cli = Ignition.startClient(
                        new ClientConfiguration()
                            .setAddresses("127.0.0.1:" + clientPort(grid(0)))
                            .setUserName("ignite")
                            .setUserPassword("ignite"))) {
                        cli.query(new SqlFieldsQuery(sql)).getAll();
                    }
                    catch (Exception e) {
                        log.error("Unexpected exception", e);
                    }
                }
            );
        };

        Consumer<String> initiatorChecker = initiatorId -> {
            assertTrue("Invalid initiator ID: " + initiatorId, Pattern.compile("cli:127\\.0\\.0\\.1:[0-9]+@ignite")
                .matcher(initiatorId).matches());
        };

        check(sqlExec, initiatorChecker);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJobDefaultInitiatorId() throws Exception {
        Consumer<String> sqlExec = sql -> {
            grid(1).cluster().forServers().ignite().compute().runAsync(new TestSqlJob(sql));
        };

        Consumer<String> initiatorChecker = initiatorId -> {
            assertTrue("Invalid initiator ID: " + initiatorId,
                initiatorId.startsWith("task:" + TestSqlJob.class.getName()) &&
                    initiatorId.endsWith(grid(1).context().localNodeId().toString()));
        };

        check(sqlExec, initiatorChecker);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJdbcV2InitiatorId() throws Exception {
        Consumer<String> sqlExec = sql -> {
            final UUID grid0NodeId = grid(0).cluster().localNode().id();

            GridTestUtils.runAsync(() -> {
                    try (Connection conn = DriverManager.getConnection(CFG_URL_PREFIX + "lazy=false:nodeId="
                        + grid0NodeId + "@modules/clients/src/test/config/jdbc-security-config.xml")) {
                        try (Statement stmt = conn.createStatement()) {
                            stmt.execute(sql);
                        }
                    }
                    catch (SQLException e) {
                        log.error("Unexpected exception", e);
                    }
                }
            );
        };

        Consumer<String> initiatorChecker = initiatorId -> {
            assertTrue("Invalid initiator ID: " + initiatorId,
                Pattern.compile("jdbc-v2:127\\.0\\.0\\.1:sqlGrid-ignite-jdbc-driver-[0-9a-fA-F-]+").matcher(initiatorId).matches());
        };

        check(sqlExec, initiatorChecker);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJdbcThinStreamerInitiatorId() throws Exception {
        final AtomicBoolean end = new AtomicBoolean();

        IgniteInternalFuture f = GridTestUtils.runAsync(() -> {
            try (Connection conn = DriverManager.getConnection(
                "jdbc:ignite:thin://127.0.0.1:" + clientPort(grid(0)) + "/?user=ignite&password=ignite")) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("CREATE TABLE T (ID INT PRIMARY KEY, VAL INT)");

                    stmt.execute("SET STREAMING ON");

                    for (int i = 0; !end.get(); ++i)
                        stmt.execute("INSERT INTO T VALUES(" + i + " , 0)");
                }
            }
            catch (SQLException e) {
                log.error("Unexpected exception", e);
            }
        });

        Consumer<String> initiatorChecker = initiatorId -> {
            assertTrue("Invalid initiator ID: " + initiatorId,
                Pattern.compile("jdbc-thin:127\\.0\\.0\\.1:[0-9]+@ignite").matcher(initiatorId).matches());
        };

        initiatorChecker.accept(initiatorId(grid(0), "INSERT", 2000));

        end.set(true);

        f.get();
    }

    /** */
    private void check(Consumer<String> sqlExec, Consumer<String> initiatorChecker) throws Exception {
        checkInitiatorId(sqlExec, initiatorChecker, "SELECT test.await()", "await");

        grid(0).context().query().querySqlFields(
            new SqlFieldsQuery("CREATE TABLE T (ID INT PRIMARY KEY, VAL INT)"), false).getAll();

        U.sleep(500);

        checkInitiatorId(sqlExec, initiatorChecker, "INSERT INTO T VALUES (0, test.await())", "await");

        checkInitiatorId(sqlExec, initiatorChecker, "UPDATE T SET VAL=test.await() WHERE ID = 0", "await");
    }

    /**
     * @throws Exception If failed.
     */
    private void checkInitiatorId(Consumer<String> sqlExecutor, Consumer<String> initiatorChecker,
        String sql, String sqlMatch)
        throws Exception {
        try {
            sqlExecutor.accept(sql);

            initiatorChecker.accept(initiatorId(grid(0), sqlMatch, 2000));
        }
        finally {
            TestSQLFunctions.unlockQuery();
        }

        checkRunningQueriesCount(grid(0), 0, 2000);
    }

    /**
     * @param node Ignite target node where query must be executed.
     * @param sqlMatch string to match SQL query with specified initiator ID.
     * @param timeout Timeout.
     * @return initiator ID.
     */
    private String initiatorId(IgniteEx node, String sqlMatch, int timeout) throws Exception {
        long t0 = U.currentTimeMillis();

        while (true) {
            if (U.currentTimeMillis() - t0 > timeout)
                fail("Timeout. Cannot find query with: " + sqlMatch);

            List<List<?>> res = node.context().query().querySqlFields(
                new SqlFieldsQuery("SELECT sql, initiator_id FROM SYS.SQL_QUERIES"), false).getAll();

            for (List<?> row : res) {
                if (((String)row.get(0)).toUpperCase().contains(sqlMatch.toUpperCase()))
                    return (String)row.get(1);
            }

            U.sleep(200);
        }
    }

    /**
     * @param node Noe to check running queries.
     * @param timeout Timeout to finish running queries.
     */
    private void checkRunningQueriesCount(IgniteEx node, int expectedQryCount, int timeout) {
        long t0 = U.currentTimeMillis();

        while (true) {
            List<List<?>> res = node.context().query().querySqlFields(
                new SqlFieldsQuery("SELECT * FROM SYS.SQL_QUERIES"), false).getAll();

            res.stream().forEach(System.out::println);

            if (res.size() == expectedQryCount + 1)
                return;

            if (U.currentTimeMillis() - t0 > timeout)
                fail("Timeout. There are unexpected running queries: " + res);
        }
    }

    /** */
    private static int clientPort(IgniteEx ign) {
        return ign.context().sqlListener().port();
    }

    /**
     * Utility class with custom SQL functions.
     */
    public static class TestSQLFunctions {
        /** */
        static final Phaser ph = new Phaser(2);

        /**
         * Recreate latches. Old latches are released.
         */
        static void unlockQuery() {
            ph.arriveAndAwaitAdvance();
        }

        /**
         * Releases cancelLatch that leeds to sending cancel Query and waits until cancel Query is fully processed.
         *
         * @return 0;
         */
        @QuerySqlFunction
        public static long await() {
            try {
                ph.arriveAndAwaitAdvance();
            }
            catch (Exception ignored) {
                // No-op.
            }

            return 0;
        }
    }

    /** */
    public static class TestSqlJob implements IgniteRunnable {
        /** */
        String sql;

        /** */
        @IgniteInstanceResource
        Ignite ign;

        /** */
        public TestSqlJob(String sql) {
            this.sql = sql;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            ((IgniteEx)ign).context().query().querySqlFields(
                new SqlFieldsQuery(sql), false).getAll();
        }
    }
}
