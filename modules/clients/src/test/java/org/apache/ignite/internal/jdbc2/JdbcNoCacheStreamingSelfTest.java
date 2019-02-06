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
import java.sql.PreparedStatement;
import java.util.Collections;
import java.util.Properties;
import org.apache.ignite.IgniteJdbcDriver;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteJdbcDriver.CFG_URL_PREFIX;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Data streaming test for thick driver and no explicit caches.
 */
public class JdbcNoCacheStreamingSelfTest extends GridCommonAbstractTest {
    /** JDBC URL. */
    private static final String BASE_URL = CFG_URL_PREFIX +
        "cache=default@modules/clients/src/test/config/jdbc-config.xml";

    /** Connection. */
    protected Connection conn;

    /** */
    protected transient IgniteLogger log;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return getConfiguration0(gridName);
    }

    /**
     * @param gridName Grid name.
     * @return Grid configuration used for starting the grid.
     * @throws Exception If failed.
     */
    private IgniteConfiguration getConfiguration0(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration<?,?> cache = defaultCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setIndexedTypes(
            Integer.class, Integer.class
        );

        cfg.setCacheConfiguration(cache);
        cfg.setLocalHost("127.0.0.1");

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);
        ipFinder.setAddresses(Collections.singleton("127.0.0.1:47500..47501"));

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(2);
    }

    /**
     * @param allowOverwrite Allow overwriting of existing keys.
     * @return Connection to use for the test.
     * @throws Exception if failed.
     */
    protected Connection createConnection(boolean allowOverwrite) throws Exception {
        Properties props = new Properties();

        props.setProperty(IgniteJdbcDriver.PROP_STREAMING, "true");
        props.setProperty(IgniteJdbcDriver.PROP_STREAMING_FLUSH_FREQ, "500");

        if (allowOverwrite)
            props.setProperty(IgniteJdbcDriver.PROP_STREAMING_ALLOW_OVERWRITE, "true");

        return DriverManager.getConnection(BASE_URL, props);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        U.closeQuiet(conn);

        ignite(0).cache(DEFAULT_CACHE_NAME).clear();

        super.afterTest();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testStreamedInsert() throws Exception {
        for (int i = 10; i <= 100; i += 10)
            ignite(0).cache(DEFAULT_CACHE_NAME).put(i, i * 100);

        try (Connection conn = createConnection(false)) {
            try (PreparedStatement stmt = conn.prepareStatement("insert into Integer(_key, _val) values (?, ?)")) {
                for (int i = 1; i <= 100; i++) {
                    stmt.setInt(1, i);
                    stmt.setInt(2, i);

                    stmt.executeUpdate();
                }
            }
        }

        U.sleep(500);

        // Now let's check it's all there.
        for (int i = 1; i <= 100; i++) {
            if (i % 10 != 0)
                assertEquals(i, grid(0).cache(DEFAULT_CACHE_NAME).get(i));
            else // All that divides by 10 evenly should point to numbers 100 times greater - see above
                assertEquals(i * 100, grid(0).cache(DEFAULT_CACHE_NAME).get(i));
        }
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testStreamedInsertWithOverwritesAllowed() throws Exception {
        for (int i = 10; i <= 100; i += 10)
            ignite(0).cache(DEFAULT_CACHE_NAME).put(i, i * 100);

        try (Connection conn = createConnection(true)) {
            try (PreparedStatement stmt = conn.prepareStatement("insert into Integer(_key, _val) values (?, ?)")) {
                for (int i = 1; i <= 100; i++) {
                    stmt.setInt(1, i);
                    stmt.setInt(2, i);

                    stmt.executeUpdate();
                }
            }
        }

        U.sleep(500);

        // Now let's check it's all there.
        // i should point to i at all times as we've turned overwrites on above.
        for (int i = 1; i <= 100; i++)
            assertEquals(i, grid(0).cache(DEFAULT_CACHE_NAME).get(i));
    }
}
