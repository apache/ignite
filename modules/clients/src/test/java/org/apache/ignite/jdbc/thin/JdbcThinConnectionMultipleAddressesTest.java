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
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.jdbc.thin.JdbcThinConnection;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.mxbean.ClientProcessorMXBean;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/**
 * JDBC driver reconnect test with multiple addresses.
 */
@SuppressWarnings("ThrowableNotThrown")
public class JdbcThinConnectionMultipleAddressesTest extends JdbcThinAbstractSelfTest {
    /** Nodes count. */
    private static final int NODES_CNT = 3;

    /** */
    private static final String URL_PORT_RANGE = "jdbc:ignite:thin://127.0.0.1:"
        + ClientConnectorConfiguration.DFLT_PORT + ".." + (ClientConnectorConfiguration.DFLT_PORT + 10);

    /** Jdbc ports. */
    private static ArrayList<Integer> jdbcPorts = new ArrayList<>();

    /**
     * @return JDBC URL.
     */
    private static String url() {
        StringBuilder sb = new StringBuilder("jdbc:ignite:thin://");

        for (int i = 0; i < NODES_CNT; i++)
            sb.append("127.0.0.1:").append(jdbcPorts.get(i)).append(',');

        return sb.toString();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setCacheConfiguration(cacheConfiguration(DEFAULT_CACHE_NAME));

        cfg.setMarshaller(new BinaryMarshaller());

        cfg.setClientConnectorConfiguration(
            new ClientConnectorConfiguration()
                .setPort(jdbcPorts.get(getTestIgniteInstanceIndex(name))));

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

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        jdbcPorts.clear();

        for (int i = 0; i < NODES_CNT; i++)
            jdbcPorts.add(ClientConnectorConfiguration.DFLT_PORT + i);

        startGrids(NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleAddressesConnect() throws Exception {
        try (Connection conn = DriverManager.getConnection(url())) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("SELECT 1");

                ResultSet rs = stmt.getResultSet();

                assertTrue(rs.next());

                assertEquals(1, rs.getInt(1));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPortRangeConnect() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL_PORT_RANGE)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("SELECT 1");

                ResultSet rs = stmt.getResultSet();

                assertTrue(rs.next());

                assertEquals(1, rs.getInt(1));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleAddressesOneNodeFailoverOnStatementExecute() throws Exception {
        checkReconnectOnStatementExecute(url(), false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleAddressesAllNodesFailoverOnStatementExecute() throws Exception {
        checkReconnectOnStatementExecute(url(), true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPortRangeAllNodesFailoverOnStatementExecute() throws Exception {
        checkReconnectOnStatementExecute(URL_PORT_RANGE, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleAddressesOneNodeFailoverOnResultSet() throws Exception {
        checkReconnectOnResultSet(url(), false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleAddressesAllNodesFailoverOnResultSet() throws Exception {
        checkReconnectOnResultSet(url(), true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPortRangeAllNodesFailoverOnResultSet() throws Exception {
        checkReconnectOnResultSet(URL_PORT_RANGE, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleAddressesOneNodeFailoverOnMeta() throws Exception {
        checkReconnectOnMeta(url(), false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleAddressesAllNodesFailoverOnMeta() throws Exception {
        checkReconnectOnMeta(url(), true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPortRangeAllNodesFailoverOnMeta() throws Exception {
        checkReconnectOnMeta(URL_PORT_RANGE, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleAddressesOneNodeFailoverOnStreaming() throws Exception {
        checkReconnectOnStreaming(url(), false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientConnectionMXBean() throws Exception {
        Connection conn = DriverManager.getConnection(URL_PORT_RANGE);

        try {
            final Statement stmt0 = conn.createStatement();

            stmt0.execute("SELECT 1");

            ResultSet rs0 = stmt0.getResultSet();

            ClientProcessorMXBean serverMxBean = null;

            // Find node which client is connected to.
            for (int i = 0; i < NODES_CNT; i++) {
                serverMxBean = clientProcessorBean(i);

                if (!serverMxBean.getConnections().isEmpty())
                    break;
            }

            assertNotNull("No ClientConnections MXBean found.", serverMxBean);

            serverMxBean.dropAllConnections();

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt0.execute("SELECT 1");

                    return null;
                }
            }, SQLException.class, "Failed to communicate with Ignite cluster");

            assertTrue(rs0.isClosed());
            assertTrue(stmt0.isClosed());

            assertTrue(getActiveClients().isEmpty());

            final Statement stmt1 = conn.createStatement();

            stmt1.execute("SELECT 1");

            ResultSet rs1 = stmt1.getResultSet();

            // Check active clients.
            List<String> activeClients = getActiveClients();

            assertEquals(1, activeClients.size());

            assertTrue(rs1.next());
            assertEquals(1, rs1.getInt(1));

            rs1.close();
            stmt1.close();
        }
        finally {
            conn.close();
        }

        boolean allClosed = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return getActiveClients().isEmpty();
            }
        }, 10_000);

        assertTrue(allClosed);
    }

    /**
     * Return active client list.
     *
     * @return clients.
     */
    @NotNull private List<String> getActiveClients() {
        List<String> activeClients = new ArrayList<>(1);

        for (int i = 0; i < NODES_CNT; i++) {
            ClientProcessorMXBean mxBean = clientProcessorBean(i);

            assertNotNull(mxBean);

            activeClients.addAll(mxBean.getConnections());
        }
        return activeClients;
    }

    /**
     * Return ClientProcessorMXBean.
     *
     * @return MBean.
     */
    private ClientProcessorMXBean clientProcessorBean(int igniteInt) {
        return getMxBean(getTestIgniteInstanceName(igniteInt), "Clients",
            ClientListenerProcessor.class, ClientProcessorMXBean.class);
    }

    /**
     * Check failover on restart cluster or stop one node.
     *
     * @param url Connection URL.
     * @param allNodes Restart all nodes flag.
     * @throws Exception If failed.
     */
    private void checkReconnectOnMeta(String url, boolean allNodes) throws Exception {
        try (Connection conn = DriverManager.getConnection(url)) {
            DatabaseMetaData meta = conn.getMetaData();

            final String[] types = {"TABLES"};

            ResultSet rs0 = meta.getTables(null, null, null, types);

            assertFalse(rs0.next());

            stop(conn, allNodes);

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    meta.getTables(null, null, null, null);

                    return null;
                }
            }, SQLException.class, "Failed to communicate with Ignite cluster");

            restart(allNodes);

            rs0 = meta.getTables(null, null, null, types);
            assertFalse(rs0.next());
        }
    }

    /**
     * Check failover on restart cluster ar stop one node.
     *
     * @param url Connection URL.
     * @param allNodes Restart all nodes flag.
     * @throws Exception If failed.
     */
    private void checkReconnectOnStatementExecute(String url, boolean allNodes) throws Exception {
        try (Connection conn = DriverManager.getConnection(url)) {
            final Statement stmt0 = conn.createStatement();

            stmt0.execute("SELECT 1");

            ResultSet rs0 = stmt0.getResultSet();

            assertTrue(rs0.next());
            assertEquals(1, rs0.getInt(1));
            assertFalse(rs0.isClosed());

            stop(conn, allNodes);

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt0.execute("SELECT 1");

                    return null;
                }
            }, SQLException.class, "Failed to communicate with Ignite cluster");

            assertTrue(rs0.isClosed());
            assertTrue(stmt0.isClosed());

            restart(allNodes);

            final Statement stmt1 = conn.createStatement();

            stmt1.execute("SELECT 1");

            ResultSet rs1 = stmt1.getResultSet();

            assertTrue(rs1.next());
            assertEquals(1, rs1.getInt(1));
        }
    }

    /**
     * Check failover on restart cluster ar stop one node.
     *
     * @param url Connection URL.
     * @param allNodes Restart all nodes flag.
     * @throws Exception If failed.
     */
    private void checkReconnectOnResultSet(String url, boolean allNodes) throws Exception {
        try (Connection conn = DriverManager.getConnection(url)) {
            final Statement stmt0 = conn.createStatement();

            stmt0.execute("SELECT 1");

            final ResultSet rs0 = stmt0.getResultSet();

            assertTrue(rs0.next());
            assertEquals(1, rs0.getInt(1));
            assertFalse(rs0.isClosed());

            stop(conn, allNodes);

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    rs0.close();

                    return null;
                }
            }, SQLException.class, "Failed to communicate with Ignite cluster");

            assertTrue(rs0.isClosed());
            assertTrue(stmt0.isClosed());

            restart(allNodes);

            final Statement stmt1 = conn.createStatement();

            stmt1.execute("SELECT 1");

            ResultSet rs1 = stmt1.getResultSet();

            assertTrue(rs1.next());
            assertEquals(1, rs1.getInt(1));
        }
    }

    /**
     * Check failover on restart cluster ar stop one node.
     *
     * @param url Connection URL.
     * @param allNodes Restart all nodes flag.
     * @throws Exception If failed.
     */
    private void checkReconnectOnStreaming(String url, boolean allNodes) throws Exception {
        try (Connection conn = DriverManager.getConnection(url)) {
            final Statement stmt0 = conn.createStatement();
            stmt0.execute("CREATE TABLE TEST(id int primary key, val int)");

            stmt0.execute("SET STREAMING 1 BATCH_SIZE 10 ALLOW_OVERWRITE 0 " +
                " PER_NODE_BUFFER_SIZE 1000 FLUSH_FREQUENCY 1000");

            final ResultSet rs0 = stmt0.getResultSet();

            stop(conn, allNodes);

            final int [] id = {0};

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    // compiled instead of while (true)
                    while (id[0] >= 0) {
                        stmt0.execute("INSERT INTO TEST(id, val) values (" + id[0] + ", " + id[0] + ")");

                        id[0]++;
                    }

                    return null;
                }
            }, SQLException.class, "Failed to communicate with Ignite cluster on JDBC streaming");

            int minId = id[0];

            restart(allNodes);

            final Statement stmt1 = conn.createStatement();

            stmt1.execute("SET STREAMING 1 BATCH_SIZE 10 ALLOW_OVERWRITE 0 " +
                " PER_NODE_BUFFER_SIZE 1000 FLUSH_FREQUENCY 1000");

            for (int i = 0; i < 10; ++i, id[0]++)
                stmt1.execute("INSERT INTO TEST(id, val) values (" + id[0] + ", " + id[0] + ")");

            stmt1.execute("SET STREAMING 0");

            stmt1.execute("SELECT ID FROM TEST WHERE id < " + minId);

            assertFalse(stmt1.getResultSet().next());

            stmt1.execute("SELECT count(id) FROM TEST WHERE id > " + minId);

            assertTrue(stmt1.getResultSet().next());
        }
    }

    /**
     * @param conn Connection.
     * @param all If {@code true} all nodes will be stopped.
     */
    private void stop(Connection conn, boolean all) {
        if (all)
            stopAllGrids();
        else {

            if (partitionAwareness) {
                for (int i = 0; i < NODES_CNT - 1; i++)
                    stopGrid(i);
            }
            else {
                int idx = ((JdbcThinConnection)conn).serverIndex();

                stopGrid(idx);
            }
        }
    }

    /**
     * @param all If {@code true} all nodes will be started.
     * @throws Exception On error.
     */
    private void restart(boolean all) throws Exception {
        if (all)
            startGrids(NODES_CNT);
    }
}
