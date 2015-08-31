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

package org.apache.ignite.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteJdbcDriver.PROP_NODE_ID;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Test JDBC with several local caches.
 */
public class JdbcLocalCachesSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** URL. */
    private static final String URL = "jdbc:ignite://127.0.0.1/" + CACHE_NAME;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cache = defaultCacheConfiguration();

        cache.setName(CACHE_NAME);
        cache.setCacheMode(LOCAL);
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setIndexedTypes(
            String.class, Integer.class
        );

        cfg.setCacheConfiguration(cache);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(2);

        IgniteCache<Object, Object> cache1 = grid(0).cache(CACHE_NAME);

        assert cache1 != null;

        cache1.put("key1", 1);
        cache1.put("key2", 2);

        IgniteCache<Object, Object> cache2 = grid(1).cache(CACHE_NAME);

        assert cache2 != null;

        cache2.put("key1", 3);
        cache2.put("key2", 4);

        Class.forName("org.apache.ignite.IgniteJdbcDriver");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCache1() throws Exception {
        Properties cfg = new Properties();

        cfg.setProperty(PROP_NODE_ID, grid(0).localNode().id().toString());

        Connection conn = null;

        try {
            conn = DriverManager.getConnection(URL, cfg);

            ResultSet rs = conn.createStatement().executeQuery("select _val from Integer order by _val");

            int cnt = 0;

            while (rs.next())
                assertEquals(++cnt, rs.getInt(1));

            assertEquals(2, cnt);
        }
        finally {
            if (conn != null)
                conn.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCache2() throws Exception {
        Properties cfg = new Properties();

        cfg.setProperty(PROP_NODE_ID, grid(1).localNode().id().toString());

        Connection conn = null;

        try {
            conn = DriverManager.getConnection(URL, cfg);

            ResultSet rs = conn.createStatement().executeQuery("select _val from Integer order by _val");

            int cnt = 0;

            while (rs.next())
                assertEquals(++cnt + 2, rs.getInt(1));

            assertEquals(2, cnt);
        }
        finally {
            if (conn != null)
                conn.close();
        }
    }
}