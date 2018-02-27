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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;

/**
 * Connection test.
 */
@SuppressWarnings("ThrowableNotThrown")
public class JdbcThinConnectionMultipleAddressesTest extends JdbcThinAbstractSelfTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Nodes count. */
    private static final int NODES_CNT = 3;

    /** */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1";

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

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setMarshaller(new BinaryMarshaller());

        cfg.setClientConnectorConfiguration(
            new ClientConnectorConfiguration().setPort(jdbcPorts.get(getTestIgniteInstanceIndex(name))));

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
    public void testMultipleAddressesConnect() throws Exception {
        try (Connection conn = DriverManager.getConnection(url())) {
            checkConnection(conn);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"EmptyTryBlock", "unused"})
    public void testFailover() throws Exception {
        try (Connection conn = DriverManager.getConnection(url())) {
            final Statement stmt0 = conn.createStatement();

            stmt0.execute("SELECT 1");

            ResultSet rs0 = stmt0.getResultSet();

            assertTrue(rs0.next());

            assertEquals(1, rs0.getInt(1));

            assertFalse(rs0.isClosed());

            stopAllGrids();

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt0.execute("SELECT 1");

                    return null;
                }
            }, SQLException.class, "Failed to communicate with Ignite cluster");

            startGrids(NODES_CNT);

            assertTrue(rs0.isClosed());
            assertTrue(stmt0.isClosed());

            final Statement stmt1 = conn.createStatement();

            stmt1.execute("SELECT 1");

            ResultSet rs1 = stmt1.getResultSet();

            assertTrue(rs1.next());

            assertEquals(1, rs1.getInt(1));

        }
    }

    /**
     * @param conn Connection.
     * @throws SQLException On error.
     */
    private void checkConnection(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("SELECT 1");

            ResultSet rs = stmt.getResultSet();

            assertTrue(rs.next());

            assertEquals(1, rs.getInt(1));
        }
    }
}