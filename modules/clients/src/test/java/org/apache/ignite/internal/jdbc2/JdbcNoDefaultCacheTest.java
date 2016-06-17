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
import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteJdbcDriver.CFG_URL_PREFIX;

/**
 *
 */
public class JdbcNoDefaultCacheTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** First cache name. */
    private static final String CACHE1_NAME = "cache1";

    /** Second cache name. */
    private static final String CACHE2_NAME = "cache2";

    /** Ignite configuration URL. */
    private static final String CFG_URL = "modules/clients/src/test/config/jdbc-config.xml";

    /** Grid count. */
    private static final int GRID_CNT = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(cacheConfiguration(CACHE1_NAME), cacheConfiguration(CACHE2_NAME));

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

        cfg.setIndexedTypes(Integer.class, Integer.class);

        cfg.setName(name);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(GRID_CNT);

        Class.forName("org.apache.ignite.IgniteJdbcDriver");

        Ignite ignite = ignite(0);

        IgniteCache<Integer, Integer> cache1 = ignite.cache(CACHE1_NAME);
        IgniteCache<Integer, Integer> cache2 = ignite.cache(CACHE2_NAME);

        for (int i = 0; i < 10; i++) {
            cache1.put(i, i * 2);
            cache2.put(i, i * 3);
        }
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
            assertTrue(((JdbcConnection)conn).ignite().configuration().isClientMode());
        }

        try (Connection conn = DriverManager.getConnection(url + '/')) {
            assertNotNull(conn);
            assertTrue(((JdbcConnection)conn).ignite().configuration().isClientMode());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoCacheNameQuery() throws Exception {
        Statement stmt;

        stmt = DriverManager.getConnection(CFG_URL_PREFIX + CFG_URL).createStatement();

        assertNotNull(stmt);
        assertFalse(stmt.isClosed());

        stmt.execute("select t._key, t._val from \"cache1\".Integer t");

        ResultSet rs = stmt.getResultSet();

        while(rs.next())
            assertEquals(rs.getInt(2), rs.getInt(1) * 2);

        stmt.execute("select t._key, t._val from \"cache2\".Integer t");

        rs = stmt.getResultSet();

        while(rs.next())
            assertEquals(rs.getInt(2), rs.getInt(1) * 3);

        stmt.execute("select t._key, t._val, v._val " +
            "from \"cache1\".Integer t join \"cache2\".Integer v on t._key = v._key");

        rs = stmt.getResultSet();

        while(rs.next()) {
            assertEquals(rs.getInt(2), rs.getInt(1) * 2);
            assertEquals(rs.getInt(3), rs.getInt(1) * 3);
        }

        stmt.close();
    }
}
