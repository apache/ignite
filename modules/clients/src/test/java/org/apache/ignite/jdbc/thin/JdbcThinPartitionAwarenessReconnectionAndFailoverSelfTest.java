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

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.jdbc.thin.AffinityCache;
import org.apache.ignite.internal.jdbc.thin.JdbcThinConnection;
import org.apache.ignite.internal.jdbc.thin.JdbcThinTcpIo;
import org.apache.ignite.internal.jdbc.thin.QualifiedSQLQuery;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionSingleNode;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Jdbc thin partition awareness reconnection and query failover test.
 */
public class JdbcThinPartitionAwarenessReconnectionAndFailoverSelfTest extends JdbcThinAbstractSelfTest {
    /** Rows count. */
    private static final int ROWS_COUNT = 100;

    /** URL. */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1:10800..10802?partitionAwareness=true";

    /** URL with port. */
    public static final String URL_WITH_ONE_PORT = "jdbc:ignite:thin://127.0.0.1:10800?partitionAwareness=true";

    /** Nodes count. */
    private static final int INITIAL_NODES_CNT = 3;

    /** Log handler. */
    private static LogHandler logHnd;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(INITIAL_NODES_CNT);

        Logger log = Logger.getLogger(JdbcThinConnection.class.getName());
        logHnd = new LogHandler();
        logHnd.setLevel(Level.ALL);
        log.setUseParentHandlers(false);
        log.addHandler(logHnd);
        log.setLevel(Level.ALL);
    }

    /**
     * Check that background connection establishment works as expected.
     * <p>
     * Within new reconnection logic in partition awareness mode when {@code JdbcThinConnection} is created it eagerly
     * establishes a connection to one and only one ignite node. All other connections to nodes specified in connection
     * properties are established by background thread.
     * <p>
     * So in given test we specify url with 3 different ports and verify that 3 connections will be created: one in
     * eager mode and two within background thread. It takes some time for background thread to create a connection, and
     * cause, in addition to that it runs periodically with some delay, {@code GridTestUtils.waitForCondition} is used
     * in order to check that all expected connections are established.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBackgroundConnectionEstablishment() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            Map<UUID, JdbcThinTcpIo> ios = GridTestUtils.getFieldValue(conn, "ios");

            assertConnectionsCount(ios, 3);
        }
    }

    /**
     * Test connection failover:
     * <ol>
     * <li>Check that at the beginning of test {@code INITIAL_NODES_CNT} connections are established.</li>
     * <li>Stop one node, invalidate dead connection (jdbc thin, won't detect that node has gone,
     * until it tries to touch it) and verify, that connections count has decremented. </li>
     * <li>Start, previously stopped node, and check that connections count also restored to initial value.</li>
     * </ol>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testConnectionFailover() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            Map<UUID, JdbcThinTcpIo> ios = GridTestUtils.getFieldValue(conn, "ios");

            assertConnectionsCount(ios, INITIAL_NODES_CNT);

            assertEquals("Unexpected connections count.", INITIAL_NODES_CNT, ios.size());

            stopGrid(1);

            invalidateConnectionToStoppedNode(conn);

            assertEquals("Unexpected connections count.", INITIAL_NODES_CNT - 1, ios.size());

            startGrid(1);

            assertConnectionsCount(ios, INITIAL_NODES_CNT);
        }
    }

    /**
     * Test total connection failover:
     * <ol>
     * <li>Check that at the beginning of test {@code INITIAL_NODES_CNT} connections are established.</li>
     * <li>Stop all nodes, invalidate dead connections (jdbc thin, won't detect that node has gone,
     * until it tries to touch it) and verify, that connections count equals to zero. </li>
     * <li>Start, previously stopped nodes, and check that connections count also restored to initial value.</li>
     * </ol>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTotalConnectionFailover() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            Map<UUID, JdbcThinTcpIo> ios = GridTestUtils.getFieldValue(conn, "ios");

            assertConnectionsCount(ios, INITIAL_NODES_CNT);

            for (int i = 0; i < INITIAL_NODES_CNT; i++) {
                stopGrid(i);
                invalidateConnectionToStoppedNode(conn);
            }

            assertConnectionsCount(ios, 0);

            for (int i = 0; i < INITIAL_NODES_CNT; i++)
                startGrid(i);

            assertConnectionsCount(ios, INITIAL_NODES_CNT);
        }
    }

    /**
     * Test eager connection failover:
     * <ol>
     * <li>Check that at the beginning of test {@code INITIAL_NODES_CNT} connections are established.</li>
     * <li>Stop all nodes, invalidate dead connections (jdbc thin, won't detect that node has gone,
     * until it tries to touch it) and verify, that connections count equals to zero. </li>
     * <li>Wait for some time, in order for reconnection thread to increase delay between connection attempts,
     * because of reconnection failures.</li>
     * <li>Start, previously stopped nodes, and send simple query immediately. Eager reconnection is expected.
     * <b>NOTE</b>:There's still a chance that connection would be recreated by background thread and not eager
     * process. In order to decrease given possibility we've waited for some time on previous step.</li>
     * <li>Ensure that after some time all connections will be restored.</li>
     * </ol>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testEagerConnectionFailover() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            Map<UUID, JdbcThinTcpIo> ios = GridTestUtils.getFieldValue(conn, "ios");

            assertConnectionsCount(ios, INITIAL_NODES_CNT);

            for (int i = 0; i < INITIAL_NODES_CNT; i++) {
                stopGrid(i);
                invalidateConnectionToStoppedNode(conn);
            }

            assertEquals("Unexpected connections count.", 0, ios.size());

            doSleep(4 * JdbcThinConnection.RECONNECTION_DELAY);

            for (int i = 0; i < INITIAL_NODES_CNT; i++)
                startGrid(i);

            conn.createStatement().execute("select 1;");

            assertConnectionsCount(ios, INITIAL_NODES_CNT);
        }
    }

    /**
     * Check that reconnection thread increases delay between unsuccessful connection attempts:
     * <ol>
     * <li>Specify two inet addresses one valid and one inoperative.</li>
     * <li>Wait for specific amount of time. The reconnection logic suppose to increase delays between reconnection
     * attempts. The basic idea is very simple: delay is doubled on evey connection failure until connection succeeds or
     * until delay exceeds predefined maximum value {@code JdbcThinConnection.RECONNECTION_MAX_DELAY}
     * <pre>
     *   |_|_ _|_ _ _ _|_ _ _ _ _ _ _ _|
     *   where: '|' is connection attempt;
     *          '_' is an amount of time that reconnection tread waits, equal to JdbcThinConnection.RECONNECTION_DELAY;
     *
     *   so if we wait for 9 * RECONNECTION_DELAY, we expect to see exact four connection attempts:
     *   |_|_ _|_ _ _ _|_ _^_ _ _ _ _ _|
     *   </pre>
     * </li>
     * <li>Check that there were exact four reconnection attempts. In order to do this, we check logs, expecting to see
     * four warning messages there.</li>
     * </ol>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectionDelayIncreasing() throws Exception {
        try (Connection ignored = DriverManager.getConnection(
            "jdbc:ignite:thin://127.0.0.1:10800,127.0.0.1:10810?partitionAwareness=true")) {
            logHnd.records.clear();

            doSleep(9 * JdbcThinConnection.RECONNECTION_DELAY);

            assertEquals("Unexpected log records count.", 4, logHnd.records.size());

            String expRecordMsg = "Failed to connect to Ignite node " +
                "[url=jdbc:ignite:thin://127.0.0.1:10800,127.0.0.1:10810]. address = [localhost/127.0.0.1:10810].";

            for (LogRecord record : logHnd.records) {
                assertEquals("Unexpected log record text.", expRecordMsg, record.getMessage());
                assertEquals("Unexpected log level", Level.WARNING, record.getLevel());
            }
        }
    }

    /**
     * Check that reconnection thread selectively increases delay between unsuccessful connection attempts:
     * <ol>
     * <li>Create {@code JdbcThinConnection} with two valid inet addresses.</li>
     * <li>Stop one node and invalidate corresponding connection. Ensure that only one connection left.</li>
     * <li>Wait for specific amount of time. The reconnection logic suppose to increase delays between reconnection
     * attempts. The basic idea is very simple: delay is doubled on evey connection failure until connection succeeds or
     * until delay exceeds predefined maximum value {@code JdbcThinConnection.RECONNECTION_MAX_DELAY}
     * <pre>
     *   |_|_ _|_ _ _ _|_ _ _ _ _ _ _ _|
     *   where: '|' is connection attempt;
     *          '_' is an amount of time that reconnection tread waits, equal to JdbcThinConnection.RECONNECTION_DELAY;
     *
     *   so if we wait for 9 * RECONNECTION_DELAY, we expect to see exact four connection attempts:
     *   |_|_ _|_ _ _ _|_ _^_ _ _ _ _ _|
     *   </pre>
     * </li>
     * <li>Check that there were exact four reconnection attempts. In order to do this, we check logs, expecting to see
     * four warning messages there.</li>
     * <li>Start previously stopped node.</li>
     * <li>Wait until next reconnection attempt.</li>
     * <li>Check that both connections are established and that there are no warning messages within logs.</li>
     * <li>One more time: stop one node and invalidate corresponding connection.
     * Ensure that only one connection left.</li>
     * <li>Wait for some time.</li>
     * <li>Ensure that delay between reconnection was reset to initial value.
     * In other words, we again expect four warning messages within logs.</li>
     * </ol>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectionDelaySelectiveIncreasing() throws Exception {
        try (Connection conn = DriverManager.getConnection(
            "jdbc:ignite:thin://127.0.0.1:10800..10801?partitionAwareness=true")) {
            // Stop one node and invalidate corresponding connection. Ensure that only one connection left.
            stopGrid(0);

            invalidateConnectionToStoppedNode(conn);

            Map<UUID, JdbcThinTcpIo> ios = GridTestUtils.getFieldValue(conn, "ios");

            assertEquals("Unexpected connections count.", 1, ios.size());

            logHnd.records.clear();

            // Wait for some specific amount of time and ensure that there were exact four reconnection attempts.
            doSleep(9 * JdbcThinConnection.RECONNECTION_DELAY);

            assertEquals("Unexpected log records count.", 4, logHnd.records.size());

            String expRecordMsg = "Failed to connect to Ignite node [url=jdbc:ignite:thin://127.0.0.1:10800..10801]." +
                " address = [localhost/127.0.0.1:10800].";

            for (LogRecord record : logHnd.records) {
                assertEquals("Unexpected log record text.", expRecordMsg, record.getMessage());
                assertEquals("Unexpected log level", Level.WARNING, record.getLevel());
            }

            // Start previously stopped node.
            startGrid(0);

            logHnd.records.clear();

            // Waiting until next reconnection attempt.
            doSleep(9 * JdbcThinConnection.RECONNECTION_DELAY);

            // Checking that both connections are established and that there are no warning messages within logs.
            assertEquals("Unexpected log records count.", 0, logHnd.records.size());

            assertEquals("Unexpected connections count.", 2, ios.size());

            // One more time: stop one node, invalidate corresponding connection and ensure that only one connection
            // left.
            stopGrid(0);

            invalidateConnectionToStoppedNode(conn);

            assertEquals("Unexpected connections count.", 1, ios.size());

            logHnd.records.clear();

            // Wait for some time and ensure that delay between reconnection was reset to initial value.
            doSleep(9 * JdbcThinConnection.RECONNECTION_DELAY);

            assertEquals("Unexpected log records count.", 4, logHnd.records.size());

            for (LogRecord record : logHnd.records) {
                assertEquals("Unexpected log record text.", expRecordMsg, record.getMessage());
                assertEquals("Unexpected log level", Level.WARNING, record.getLevel());
            }

            startGrid(0);
        }
    }

    /**
     * Check that failover doesn't occur if the result of sending sql request is SQLException.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableNotThrown")
    @Test
    public void testSQLExceptionFailover() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            logHnd.records.clear();

            GridTestUtils.assertThrows(null,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.createStatement().execute("select invalid column name.");

                        return null;
                    }
                },
                SQLException.class,
                "Failed to parse query."
            );
        }

        assertEquals("Unexpected log records count.", 1, logHnd.records.size());

        LogRecord record = logHnd.records.get(0);

        assertEquals("Unexpected log record text.", "Exception during sending an sql request.",
            record.getMessage());

        assertEquals("Unexpected log level", Level.FINE, record.getLevel());
    }

    /**
     * Check that failover occurs if the result of sending first iteration of sql request is an Exception but not an
     * SQLException.
     *
     * <ol>
     * <li>Create {@code JdbcThinConnection} to all existing nodes.</li>
     * <li>Create a cache and populate it with some data.</li>
     * <li>Submit some failover-applicable sql query with specific condition within where clause,
     * that assumes partition awareness. Submit same query one more time. It's necessary in order to warm up affinity
     * awareness cache.</li>
     * <li>From within affinity cache calculate node that was used to process query. Stop it.</li>
     * <li>Submit sql query, that is equal to initial one, one more time.
     * Because of partition awareness, given query will be routed to previously stopped node, so an Exception will be
     * received. Here query failover goes and resents query to an alive node using another {@code JdbcThinTcpIo}</li>
     * <li>Because of failover, no exceptions are expected on Jdbc thin client side.
     * However within the {@code JdbcThinConnection}, in case of {@code Level.FINE} log level, corresponding log record
     * is expected.</li>
     * </ol>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testQueryFailover() throws Exception {
        try (Connection conn = DriverManager.getConnection(
            "jdbc:ignite:thin://127.0.0.1:10800..10802?partitionAwareness=true")) {

            final String cacheName = UUID.randomUUID().toString().substring(0, 6);

            final String sql = "select * from \"" + cacheName + "\".Person where _key = 1";

            CacheConfiguration<Object, Object> cache = prepareCacheConfig(cacheName);

            ignite(0).createCache(cache);

            fillCache(cacheName);

            Statement stmt = conn.createStatement();

            stmt.execute(sql);
            stmt.execute(sql);

            AffinityCache affinityCache = GridTestUtils.getFieldValue(conn, "affinityCache");

            Integer part = ((PartitionSingleNode)affinityCache.partitionResult(
                new QualifiedSQLQuery("PUBLIC", sql)).partitionResult().tree()).value();

            UUID nodeId = affinityCache.cacheDistribution(GridCacheUtils.cacheId(cacheName))[part];

            int gridIdx = new Integer(Ignition.ignite(nodeId).name().substring(getTestIgniteInstanceName().length()));
            stopGrid(gridIdx);

            logHnd.records.clear();

            conn.createStatement().execute(sql);

            startGrid(gridIdx);
        }

        assertEquals("Unexpected log records count.", 1, logHnd.records.size());

        LogRecord record = logHnd.records.get(0);

        assertEquals("Unexpected log record text.", "Exception during sending an sql request.",
            record.getMessage());

        assertEquals("Unexpected log level", Level.FINE, record.getLevel());
    }

    /**
     * Check that all possible sub-connections are used.
     *
     * <ol>
     * <li>Create {@code JdbcThinConnection} to all existing nodes.</li>
     * <li>Stop all nodes.</li>
     * <li>Submit arbitrary sql query.</li>
     * <li>Several retries are expected. Exact number of retries should be equal to the number of originally
     * established connections. At the very end, after trying to establish brand new connections {@code SQLException}
     * with message: 'Failed to connect to server' should be thrown.</li>
     * </ol>
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableNotThrown")
    @Test
    public void testFailoverOnAllNodes() throws Exception {
        try (Connection conn = DriverManager.getConnection(
            "jdbc:ignite:thin://127.0.0.1:10800..10802?partitionAwareness=true")) {
            Map<UUID, JdbcThinTcpIo> ios = GridTestUtils.getFieldValue(conn, "ios");

            assertConnectionsCount(ios, 3);

            stopAllGrids();

            logHnd.records.clear();

            GridTestUtils.assertThrows(null,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.createStatement().execute("select 1");

                        return null;
                    }
                },
                SQLException.class,
                "Failed to connect to server [url=jdbc:ignite:thin://127.0.0.1:10800..10802]"
            );
        }

        assertEquals("Unexpected log records count.", 3, logHnd.records.size());

        for (LogRecord record : logHnd.records) {
            assertEquals("Unexpected log record text.", "Exception during sending an sql request.",
                record.getMessage());
            assertEquals("Unexpected log level", Level.FINE, record.getLevel());
        }

        startGridsMultiThreaded(INITIAL_NODES_CNT);
    }

    /**
     * Check that there won't be more than 5 retry attempts.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableNotThrown")
    @Test
    public void testFailoverLimit() throws Exception {
        startGrid(3);
        startGrid(4);
        startGrid(5);

        try (Connection conn = DriverManager.getConnection(
            "jdbc:ignite:thin://127.0.0.1:10800..10805?partitionAwareness=true")) {

            Map<UUID, JdbcThinTcpIo> ios = GridTestUtils.getFieldValue(conn, "ios");

            assertConnectionsCount(ios, 6);

            stopAllGrids();

            logHnd.records.clear();

            GridTestUtils.assertThrows(null,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.createStatement().execute("select 1");

                        return null;
                    }
                },
                SQLException.class,
                "Failed to communicate with Ignite cluster."
            );

            assertEquals("Unexpected connections count.", 1, ios.keySet().size());
        }

        assertEquals("Unexpected log records count.", 5, logHnd.records.size());

        for (LogRecord record : logHnd.records) {
            assertEquals("Unexpected log record text.", "Exception during sending an sql request.",
                record.getMessage());
            assertEquals("Unexpected log level", Level.FINE, record.getLevel());
        }

        startGridsMultiThreaded(INITIAL_NODES_CNT);
    }

    /**
     * Check that there are no retries in case of transactional query.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings({"unchecked", "ThrowableNotThrown"})
    @Test
    public void testTransactionalQueryFailover() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_WITH_ONE_PORT)) {
            final String cacheName = UUID.randomUUID().toString().substring(0, 6);

            final String sql = "select 1 from \"" + cacheName + "\".Person";

            CacheConfiguration<Object, Object> cache = defaultCacheConfiguration().setName(cacheName).
                setNearConfiguration(null).setIndexedTypes(Integer.class, Person.class).
                setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT);

            ignite(0).createCache(cache);

            Statement stmt = conn.createStatement();

            stmt.execute("BEGIN");

            stmt.execute(sql);

            stopGrid(0);

            logHnd.records.clear();

            GridTestUtils.assertThrows(null,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        stmt.execute(sql);

                        return null;
                    }
                },
                SQLException.class,
                "Failed to communicate with Ignite cluster."
            );
        }

        assertEquals("Unexpected log records count.", 1, logHnd.records.size());

        LogRecord record = logHnd.records.get(0);

        assertEquals("Unexpected log record text.", "Exception during sending an sql request.",
            record.getMessage());

        assertEquals("Unexpected log level", Level.FINE, record.getLevel());

        startGrid(0);
    }

    /**
     * Check that there are no retries in following cases:
     * <ul>
     * <li>Result set's metadata request.</li>
     * <li>Multi-statements request.</li>
     * <li>DDL.</li>
     * <li>DML.</li>
     * </ul>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNoRetriesOccurred() throws Exception {
        // Check that there are no retries in case of result set's metadata request.
        checkNoRetriesOccurred(() -> {
            try (Connection conn = DriverManager.getConnection(URL_WITH_ONE_PORT)) {
                Statement stmt = conn.createStatement();

                ResultSet rs = stmt.executeQuery("select 1");

                stopGrid(0);

                rs.getMetaData();
            }
            return null;
        });

        startGrid(0);

        // Check that there are no retries in case of multi-statements request.
        checkNoRetriesOccurred(() -> {
            try (Connection conn = DriverManager.getConnection(URL_WITH_ONE_PORT)) {
                Statement stmt = conn.createStatement();

                stopGrid(0);

                stmt.executeQuery("select 1; select 2");
            }
            return null;
        });

        startGrid(0);

        // Check that there are no retries in case of DDL.
        checkNoRetriesOccurred(() -> {
            try (Connection conn = DriverManager.getConnection(URL_WITH_ONE_PORT)) {
                Statement stmt = conn.createStatement();

                stopGrid(0);

                stmt.execute("CREATE TABLE PARENT" + UUID.randomUUID().toString().substring(0, 6) +
                    " (ID INT, NAME VARCHAR, PRIMARY KEY(ID));");
            }
            return null;
        });

        startGrid(0);

        // Check that there are no retries in case of DML.
        checkNoRetriesOccurred(() -> {
            try (Connection conn = DriverManager.getConnection(URL_WITH_ONE_PORT)) {
                Statement stmt = conn.createStatement();

                String tblName = "PARENT" + UUID.randomUUID().toString().substring(0, 6);

                stmt.execute("CREATE TABLE " + tblName +
                    " (ID INT, NAME VARCHAR, PRIMARY KEY(ID));");

                stopGrid(0);

                stmt.execute("INSERT INTO" + tblName + " (ID, NAME) VALUES(1, 'aaa')");
            }
            return null;
        });

        startGrid(0);
    }

    /**
     * Check that there are retries in case of following metadata requests:
     * <ul>
     * <li>META_TABLES</li>
     * <li>META_COLUMNS</li>
     * <li>META_INDEXES</li>
     * <li>META_PARAMS</li>
     * <li>META_PRIMARY_KEYS</li>
     * <li>META_SCHEMAS</li>
     * </ul>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMetadataQueries() throws Exception {
        // Test META_TABLES query.
        checkRetriesOccurred(() -> {
            try (Connection conn = DriverManager.getConnection(URL_WITH_ONE_PORT)) {
                stopGrid(0);

                conn.getMetaData().getTables(null, null, null, null);
            }

            return null;
        });

        startGrid(0);

        // Test META_COLUMNS query.
        checkRetriesOccurred(() -> {
            try (Connection conn = DriverManager.getConnection(URL_WITH_ONE_PORT)) {
                stopGrid(0);

                conn.getMetaData().getColumns(null, null, null,
                    null);
            }

            return null;
        });

        startGrid(0);

        // Test META_INDEXES query.
        checkRetriesOccurred(() -> {
            try (Connection conn = DriverManager.getConnection(URL_WITH_ONE_PORT)) {
                stopGrid(0);

                conn.getMetaData().getIndexInfo(null, null, null, false, false);
            }

            return null;
        });

        startGrid(0);

        // Test META_PARAMS query.
        checkRetriesOccurred(() -> {
            try (Connection conn = DriverManager.getConnection(URL_WITH_ONE_PORT)) {
                stopGrid(0);

                PreparedStatement preparedStmt = conn.prepareStatement("select 1");

                preparedStmt.getParameterMetaData();
            }

            return null;
        });

        startGrid(0);

        // Test META_PRIMARY_KEYS query.
        checkRetriesOccurred(() -> {
            try (Connection conn = DriverManager.getConnection(URL_WITH_ONE_PORT)) {
                stopGrid(0);

                conn.getMetaData().getPrimaryKeys(null, null, null);
            }

            return null;
        });

        startGrid(0);

        // Test META_SCHEMAS query.
        checkRetriesOccurred(() -> {
            try (Connection conn = DriverManager.getConnection(URL_WITH_ONE_PORT)) {
                stopGrid(0);

                conn.getMetaData().getSchemas(null, null);
            }

            return null;
        });

        startGrid(0);
    }

    /**
     * Helper method in order to check that retries do have occurred in case of running {@param queriesToTest}
     * statements.
     *
     * @param queriesToTest Statements to test.
     */
    @SuppressWarnings("ThrowableNotThrown")
    private void checkRetriesOccurred(Callable queriesToTest) {
        logHnd.records.clear();

        GridTestUtils.assertThrows(null,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    queriesToTest.call();

                    return null;
                }
            },
            SQLException.class,
            "Failed to connect to server [host=localhost, port=10800]"
        );

        assertEquals("Unexpected log records count.", 1, logHnd.records.size());

        LogRecord record = logHnd.records.get(0);

        assertEquals("Unexpected log record text.", "Exception during sending an sql request.",
            record.getMessage());

        assertEquals("Unexpected log level", Level.FINE, record.getLevel());
    }

    /**
     * Helper method in order to check that no retries have occurred in case of running {@param queriesToTest}
     * statements.
     *
     * @param queriesToTest Statements to test.
     */
    @SuppressWarnings("ThrowableNotThrown")
    private void checkNoRetriesOccurred(Callable queriesToTest) {
        logHnd.records.clear();

        GridTestUtils.assertThrows(null,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    queriesToTest.call();

                    return null;
                }
            },
            SQLException.class,
            "Failed to communicate with Ignite cluster."
        );

        assertEquals("Unexpected log records count.", 1, logHnd.records.size());

        LogRecord record = logHnd.records.get(0);

        assertEquals("Unexpected log record text.", "Exception during sending an sql request.",
            record.getMessage());

        assertEquals("Unexpected log level", Level.FINE, record.getLevel());
    }

    /**
     * Assert connections count.
     *
     * @param ios Map that holds connections.
     * @param expConnCnt Expected connections count.
     */
    private void assertConnectionsCount(Map<UUID, JdbcThinTcpIo> ios, int expConnCnt)
        throws IgniteInterruptedCheckedException {
        boolean allConnectionsEstablished = GridTestUtils.waitForCondition(() -> ios.size() == expConnCnt,
            10_000);

        assertTrue("Unexpected connections count.", allConnectionsEstablished);
    }

    /**
     * Invalidate connection to stopped node. Jdbc thin, won't detect that node has gone, until it tries to touch it. So
     * sending simple query to randomly chosen connection(socket), sooner or later, will touch dead one, and thus
     * invalidate it. Please, pay attention, that it's better to send non-failoverable query, for example query with
     * ';' somewhere in the middle.
     *
     * @param conn Connections.
     */
    private void invalidateConnectionToStoppedNode(Connection conn) {
        while (true) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("select ';';");
            }
            catch (SQLException e) {
                return;
            }
        }
    }

    /**
     * Simple {@code java.util.logging.Handler} implementation in order to check log records generated by {@code
     * JdbcThinConnection}.
     */
    static class LogHandler extends Handler {

        /** Log records. */
        private final List<LogRecord> records = new ArrayList<>();

        /** {@inheritDoc} */
        @Override public void publish(LogRecord record) {
            records.add(record);
        }

        /** {@inheritDoc} */
        @Override public void close() {
        }

        /** {@inheritDoc} */
        @Override public void flush() {
        }

        /**
         * @return Records.
         */
        @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType") public List<LogRecord> records() {
            return records;
        }
    }

    /**
     * Prepares default cache configuration with given name.
     *
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    @SuppressWarnings("unchecked")
    protected CacheConfiguration<Object, Object> prepareCacheConfig(String cacheName) {
        CacheConfiguration<Object, Object> cache = defaultCacheConfiguration();

        cache.setName(cacheName);
        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setIndexedTypes(
            Integer.class, Person.class
        );

        return cache;
    }

    /**
     * Fills cache with test data.
     *
     * @param cacheName Cache name.
     */
    private void fillCache(String cacheName) {
        IgniteCache<Integer, Person> cachePerson = grid(0).cache(cacheName);

        assert cachePerson != null;

        for (int i = 0; i < ROWS_COUNT; i++)
            cachePerson.put(i, new Person(i, "John" + i, "White" + i, i + 1));
    }

    /**
     * Person.
     */
    @SuppressWarnings("unused")
    private static class Person implements Serializable {
        /** ID. */
        @QuerySqlField
        private final int id;

        /** First name. */
        @QuerySqlField
        private final String firstName;

        /** Last name. */
        @QuerySqlField
        private final String lastName;

        /** Age. */
        @QuerySqlField
        private final int age;

        /**
         * @param id ID.
         * @param firstName First name.
         * @param lastName Last name.
         * @param age Age.
         */
        private Person(int id, String firstName, String lastName, int age) {
            assert !F.isEmpty(firstName);
            assert !F.isEmpty(lastName);
            assert age > 0;

            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
            this.age = age;
        }
    }
}
