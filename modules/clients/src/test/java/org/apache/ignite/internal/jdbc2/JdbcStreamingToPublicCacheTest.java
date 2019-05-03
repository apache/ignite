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
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.Properties;
import org.apache.ignite.IgniteJdbcDriver;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteJdbcDriver.CFG_URL_PREFIX;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Data streaming test.
 */
public class JdbcStreamingToPublicCacheTest extends GridCommonAbstractTest {
    /** JDBC URL. */
    private static final String BASE_URL = CFG_URL_PREFIX + "cache=%s@modules/clients/src/test/config/jdbc-config.xml";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
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
     * @param cacheName Cache name.
     * @param streaming Streaming mode flag.
     * @return Connection to use for the test.
     * @throws Exception if failed.
     */
    private Connection createConnection(String cacheName, boolean streaming) throws Exception {
        Properties props = new Properties();

        if (streaming) {
            props.setProperty(IgniteJdbcDriver.PROP_STREAMING, "true");
            props.setProperty(IgniteJdbcDriver.PROP_STREAMING_FLUSH_FREQ, "500");
        }

        return DriverManager.getConnection(String.format(BASE_URL, cacheName), props);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ignite(0).cache(DEFAULT_CACHE_NAME).clear();

        super.afterTest();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testStreamedInsert() throws Exception {
        // Create table
        try (Connection conn = createConnection(DEFAULT_CACHE_NAME, false)) {
            Statement stmt = conn.createStatement();

            stmt.execute("create table PUBLIC.STREAM_TEST (ID int primary key, str_val varchar)");
        }

        // Fill table with streaming
        try (Connection conn = createConnection("SQL_PUBLIC_STREAM_TEST", true)) {
            PreparedStatement pstmt = conn.prepareStatement("insert into STREAM_TEST(id, str_val) values (?, ?)");

            for (int i = 1; i <= 100; i++) {
                pstmt.setInt(1, i);
                pstmt.setString(2, "val_" + i);

                pstmt.executeUpdate();
            }
        }

        // Check table's data
        try (Connection conn = createConnection("SQL_PUBLIC_STREAM_TEST", false)) {
            ResultSet rs = conn.createStatement().executeQuery("select id, str_val from STREAM_TEST");

            int cnt = 0;

            while (rs.next()) {
                assertEquals("val_" + rs.getInt(1), rs.getString(2));

                cnt++;
            }

            assertEquals(100, cnt);
        }
    }
}
