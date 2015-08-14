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

import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import org.jetbrains.annotations.*;

import java.sql.*;
import java.util.concurrent.*;

import static org.apache.ignite.IgniteJdbcDriver.*;

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

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(cacheConfiguration(null), cacheConfiguration(CUSTOM_CACHE_NAME));

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

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
        startGridsMultiThreaded(2);

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
