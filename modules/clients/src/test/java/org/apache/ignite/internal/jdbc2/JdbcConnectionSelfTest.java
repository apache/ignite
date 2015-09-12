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

package org.apache.ignite.internal.jdbc2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteJdbcDriver.CFG_URL_PREFIX;

/**
 * Connection test.
 */
public class JdbcConnectionSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Custom cache name. */
    private static final String CUSTOM_CACHE_NAME = "custom-cache";

    /** Ignite configuration URL. */
    private static final String CFG_URL = "modules/clients/src/test/config/jdbc-config.xml";

    /** Grid count. */
    private static final int GRID_CNT = 2;

    /** Daemon node flag. */
    private boolean daemon;

    /** Client node flag. */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(cacheConfiguration(null), cacheConfiguration(CUSTOM_CACHE_NAME));

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setDaemon(daemon);

        cfg.setClientMode(client);

        return cfg;
    }

    /**
     * @param name Cache name.
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    private CacheConfiguration cacheConfiguration(@Nullable String name) throws Exception {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setName(name);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(GRID_CNT);

        Class.forName("org.apache.ignite.IgniteJdbcDriver");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testDefaults() throws Exception {
        String url = CFG_URL_PREFIX + CFG_URL;

        try (Connection conn = DriverManager.getConnection(url)) {
            assertNotNull(conn);
        }

        try (Connection conn = DriverManager.getConnection(url + '/')) {
            assertNotNull(conn);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeId() throws Exception {
        String url = CFG_URL_PREFIX + "nodeId=" + grid(0).localNode().id() + '@' + CFG_URL;

        try (Connection conn = DriverManager.getConnection(url)) {
            assertNotNull(conn);
        }

        url = CFG_URL_PREFIX + "cache=" + CUSTOM_CACHE_NAME + ":nodeId=" + grid(0).localNode().id() + '@' + CFG_URL;

        try (Connection conn = DriverManager.getConnection(url)) {
            assertNotNull(conn);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testWrongNodeId() throws Exception {
        UUID wrongId = UUID.randomUUID();

        final String url = CFG_URL_PREFIX + "nodeId=" + wrongId + '@' + CFG_URL;

        GridTestUtils.assertThrows(
                log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        try (Connection conn = DriverManager.getConnection(url)) {
                            return conn;
                        }
                    }
                },
                SQLException.class,
                "Failed to establish connection with node (is it a server node?): " + wrongId
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientNodeId() throws Exception {
        client = true;

        IgniteEx client = (IgniteEx)startGrid();

        UUID clientId = client.localNode().id();

        final String url = CFG_URL_PREFIX + "nodeId=" + clientId + '@' + CFG_URL;

        GridTestUtils.assertThrows(
                log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        try (Connection conn = DriverManager.getConnection(url)) {
                            return conn;
                        }
                    }
                },
                SQLException.class,
                "Failed to establish connection with node (is it a server node?): " + clientId
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testDaemonNodeId() throws Exception {
        daemon = true;

        IgniteEx daemon = startGrid(GRID_CNT);

        UUID daemonId = daemon.localNode().id();

        final String url = CFG_URL_PREFIX + "nodeId=" + daemonId + '@' + CFG_URL;

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    try (Connection conn = DriverManager.getConnection(url)) {
                        return conn;
                    }
                }
            },
            SQLException.class,
            "Failed to establish connection with node (is it a server node?): " + daemonId
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testCustomCache() throws Exception {
        String url = CFG_URL_PREFIX + "cache=" + CUSTOM_CACHE_NAME + '@' + CFG_URL;

        try (Connection conn = DriverManager.getConnection(url)) {
            assertNotNull(conn);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testWrongCache() throws Exception {
        final String url = CFG_URL_PREFIX + "cache=wrongCacheName@" + CFG_URL;

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    try (Connection conn = DriverManager.getConnection(url)) {
                        return conn;
                    }
                }
            },
            SQLException.class,
            "Client is invalid. Probably cache name is wrong."
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testClose() throws Exception {
        String url = CFG_URL_PREFIX + CFG_URL;

        try(final Connection conn = DriverManager.getConnection(url)) {
            assertNotNull(conn);
            assertFalse(conn.isClosed());

            conn.close();

            assertTrue(conn.isClosed());

            GridTestUtils.assertThrows(
                log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.isValid(2);

                        return null;
                    }
                },
                SQLException.class,
                "Connection is closed."
            );
        }
    }
}
