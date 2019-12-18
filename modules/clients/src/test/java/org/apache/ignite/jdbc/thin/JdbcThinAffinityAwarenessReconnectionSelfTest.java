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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.jdbc.thin.JdbcThinConnection;
import org.apache.ignite.internal.jdbc.thin.JdbcThinTcpIo;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Jdbc thin affinity awareness reconnection test.
 */
public class JdbcThinAffinityAwarenessReconnectionSelfTest extends JdbcThinAbstractSelfTest {
    /** URL. */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1:10800..10802?affinityAwareness=true";

    /** Nodes count. */
    private static final int INITIAL_NODES_CNT = 3;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(INITIAL_NODES_CNT);
    }

    /**
     * Check that background connection establishment works as expected.
     * <p>
     * Within new reconnection logic in affinity awareness mode when {@code JdbcThinConnection} is created
     * it eagerly establishes a connection to one and only one ignite node. All other connections to nodes specified in
     * connection properties are established by background thread.
     * <p>
     * So in given test we specify url with 3 different ports and verify that 3 connections will be created:
     * one in eager mode and two within background thread. It takes some time for background thread to create
     * a connection, and cause, in addition to that it runs periodically with some delay,
     * {@code GridTestUtils.waitForCondition} is used in order to check that all expected connections are established.
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
     *   until it tries to touch it) and verify, that connections count has decremented. </li>
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
     *   until it tries to touch it) and verify, that connections count equals to zero. </li>
     * <li>Start, previously stopped nodes, and check that connections count also restored to initial value.</li>
     * </ol>
     *
     * @throws Exception If failed.
     */
    @Test
    public void  testTotalConnectionFailover() throws Exception {
        try(Connection conn = DriverManager.getConnection(URL)) {
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
     *   until it tries to touch it) and verify, that connections count equals to zero. </li>
     * <li>Wait for some time, in order for reconnection thread to increase delay between connection attempts,
     *   because of reconnection failures.</li>
     * <li>Start, previously stopped nodes, and send simple query immediately. Eager reconnection is expected.
     * <b>NOTE</b>:There's still a chance that connection would be recreated by background thread and not eager process.
     *   In order to decrease given possibility we've waited for some time on previous step.</li>
     * <li>Ensure that after some time all connections will be restored.</li>
     * </ol>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testEagerConnectionFailover() throws Exception {
        try(Connection conn = DriverManager.getConnection(URL)) {
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
     *   attempts. The basic idea is very simple: delay is doubled on evey connection failure until connection succeeds
     *   or until delay exceeds predefined maximum value {@code JdbcThinConnection.RECONNECTION_MAX_DELAY}
     *   <pre>
     *   |_|_ _|_ _ _ _|_ _ _ _ _ _ _ _|
     *   where: '|' is connection attempt;
     *          '_' is an amount of time that reconnection tread waits, equal to JdbcThinConnection.RECONNECTION_DELAY;
     *
     *   so if we wait for 9 * RECONNECTION_DELAY, we expect to see exact four connection attempts:
     *   |_|_ _|_ _ _ _|_ _^_ _ _ _ _ _|
     *   </pre>
     *   </li>
     * <li>Check that there were exact four reconnection attempts. In order to do this, we check logs, expecting to see
     * four warning messages there.</li>
     * </ol>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectionDelayIncreasing() throws Exception {
        Logger log = Logger.getLogger(JdbcThinConnection.class.getName());
        LogHandler hnd = new LogHandler();
        hnd.setLevel(Level.ALL);
        log.setUseParentHandlers(false);
        log.addHandler(hnd);
        log.setLevel(Level.ALL);

        try (Connection ignored = DriverManager.getConnection(
            "jdbc:ignite:thin://127.0.0.1:10800,127.0.0.1:10810?affinityAwareness=true")) {
            hnd.records.clear();

            doSleep(9 * JdbcThinConnection.RECONNECTION_DELAY);

            assertEquals("Unexpected log records count.", 4, hnd.records.size());

            String expRecordMsg = "Failed to connect to Ignite node " +
                "[url=jdbc:ignite:thin://127.0.0.1:10800,127.0.0.1:10810]. address = [localhost/127.0.0.1:10810].";

            for (LogRecord record: hnd.records) {
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
     *   attempts. The basic idea is very simple: delay is doubled on evey connection failure until connection succeeds
     *   or until delay exceeds predefined maximum value {@code JdbcThinConnection.RECONNECTION_MAX_DELAY}
     *   <pre>
     *   |_|_ _|_ _ _ _|_ _ _ _ _ _ _ _|
     *   where: '|' is connection attempt;
     *          '_' is an amount of time that reconnection tread waits, equal to JdbcThinConnection.RECONNECTION_DELAY;
     *
     *   so if we wait for 9 * RECONNECTION_DELAY, we expect to see exact four connection attempts:
     *   |_|_ _|_ _ _ _|_ _^_ _ _ _ _ _|
     *   </pre>
     *   </li>
     * <li>Check that there were exact four reconnection attempts. In order to do this, we check logs, expecting to see
     *   four warning messages there.</li>
     * <li>Start previously stopped node.</li>
     * <li>Wait until next reconnection attempt.</li>
     * <li>Check that both connections are established and that there are no warning messages within logs.</li>
     * <li>One more time: stop one node and invalidate corresponding connection.
     *   Ensure that only one connection left.</li>
     * <li>Wait for some time.</li>
     * <li>Ensure that delay between reconnection was reset to initial value.
     *   In other words, we again expect four warning messages within logs.</li>
     * </ol>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectionDelaySelectiveIncreasing() throws Exception {
        Logger log = Logger.getLogger(JdbcThinConnection.class.getName());
        LogHandler hnd = new LogHandler();
        hnd.setLevel(Level.ALL);
        log.setUseParentHandlers(false);
        log.addHandler(hnd);
        log.setLevel(Level.ALL);

        try (Connection conn = DriverManager.getConnection(
            "jdbc:ignite:thin://127.0.0.1:10800..10801?affinityAwareness=true")) {
            // Stop one node and invalidate corresponding connection. Ensure that only one connection left.
            stopGrid(0);

            invalidateConnectionToStoppedNode(conn);

            Map<UUID, JdbcThinTcpIo> ios = GridTestUtils.getFieldValue(conn, "ios");

            assertEquals("Unexpected connections count.", 1, ios.size());

            hnd.records.clear();

            // Wait for some specific amount of time and ensure that there were exact four reconnection attempts.
            doSleep(9 * JdbcThinConnection.RECONNECTION_DELAY);

            assertEquals("Unexpected log records count.", 4, hnd.records.size());

            String expRecordMsg = "Failed to connect to Ignite node [url=jdbc:ignite:thin://127.0.0.1:10800..10801]." +
                " address = [localhost/127.0.0.1:10800].";

            for (LogRecord record: hnd.records) {
                assertEquals("Unexpected log record text.", expRecordMsg, record.getMessage());
                assertEquals("Unexpected log level", Level.WARNING, record.getLevel());
            }

            // Start previously stopped node.
            startGrid(0);

            hnd.records.clear();

            // Waiting until next reconnection attempt.
            doSleep(9 * JdbcThinConnection.RECONNECTION_DELAY);

            // Checking that both connections are established and that there are no warning messages within logs.
            assertEquals("Unexpected log records count.", 0, hnd.records.size());

            assertEquals("Unexpected connections count.", 2, ios.size());

            // One more time: stop one node, invalidate corresponding connection and ensure that only one connection
            // left.
            stopGrid(0);

            invalidateConnectionToStoppedNode(conn);

            assertEquals("Unexpected connections count.", 1, ios.size());

            hnd.records.clear();

            // Wait for some time and ensure that delay between reconnection was reset to initial value.
            doSleep(9 * JdbcThinConnection.RECONNECTION_DELAY);

            assertEquals("Unexpected log records count.", 4, hnd.records.size());

            for (LogRecord record: hnd.records) {
                assertEquals("Unexpected log record text.", expRecordMsg, record.getMessage());
                assertEquals("Unexpected log level", Level.WARNING, record.getLevel());
            }

            startGrid(0);
        }
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
     * Invalidate connection to stopped node. Jdbc thin, won't detect that node has gone, until it tries to touch it.
     * So sending simple query to randomly chosen connection(socket), sooner or later, will touch dead one,
     * and thus invalidate it.
     *
     * @param conn Connections.
     */
    private void invalidateConnectionToStoppedNode(Connection conn) {
        while (true) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("select 1");
            }
            catch (SQLException e) {
                return;
            }
        }
    }

    /**
     * Simple {@code java.util.logging.Handler} implementation in order to check log records
     * generated by {@code JdbcThinConnection}.
     */
    static class LogHandler extends Handler {

        /** Log records. */
        private final List<LogRecord> records = new ArrayList<>();

        /** {@inheritDoc} */
        @Override public void publish(LogRecord record) {
            records.add(record);
        }

        /** {@inheritDoc} */
        @Override
        public void close() {
        }

        /** {@inheritDoc} */
        @Override
        public void flush() {
        }

        /**
         * @return Records.
         */
        @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType") public List<LogRecord> records() {
            return records;
        }
    }
}
