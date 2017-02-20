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
import java.util.Properties;
import org.apache.ignite.IgniteJdbcDriver;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteJdbcDriver.CFG_URL_PREFIX;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Data streaming test.
 */
public class JdbcStreamingSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** JDBC URL. */
    private static final String BASE_URL = CFG_URL_PREFIX + "modules/clients/src/test/config/jdbc-config.xml";

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

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(3);

        Class.forName("org.apache.ignite.IgniteJdbcDriver");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @param allowOverwrite Allow overwriting of existing keys.
     * @return Connection to use for the test.
     * @throws Exception if failed.
     */
    private Connection createConnection(boolean allowOverwrite) throws Exception {
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

        ignite(0).cache(null).clear();

        super.afterTest();
    }

    /**
     * @throws Exception if failed.
     */
    public void testStreamedInsert() throws Exception {
        conn = createConnection(false);

        ignite(0).cache(null).put(5, 500);

        PreparedStatement stmt = conn.prepareStatement("insert into Integer(_key, _val) values (?, ?)");

        for (int i = 1; i <= 100000; i++) {
            stmt.setInt(1, i);
            stmt.setInt(2, i);

            stmt.executeUpdate();
        }

        // Data is not there yet.
        assertNull(grid(0).cache(null).get(100000));

        // Let the stream flush.
        U.sleep(1500);

        // Now let's check it's all there.
        assertEquals(1, grid(0).cache(null).get(1));
        assertEquals(100000, grid(0).cache(null).get(100000));

        // 5 should still point to 500.
        assertEquals(500, grid(0).cache(null).get(5));
    }

    /**
     * @throws Exception if failed.
     */
    public void testStreamedInsertWithOverwritesAllowed() throws Exception {
        conn = createConnection(true);

        ignite(0).cache(null).put(5, 500);

        PreparedStatement stmt = conn.prepareStatement("insert into Integer(_key, _val) values (?, ?)");

        for (int i = 1; i <= 100000; i++) {
            stmt.setInt(1, i);
            stmt.setInt(2, i);

            stmt.executeUpdate();
        }

        // Data is not there yet.
        assertNull(grid(0).cache(null).get(100000));

        // Let the stream flush.
        U.sleep(1500);

        // Now let's check it's all there.
        assertEquals(1, grid(0).cache(null).get(1));
        assertEquals(100000, grid(0).cache(null).get(100000));

        // 5 should now point to 5 as we've turned overwriting on.
        assertEquals(5, grid(0).cache(null).get(5));
    }
}
