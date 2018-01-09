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
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.Callable;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.ignite.configuration.CacheConfiguration;
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
public class JdbcThinMetaIoTest extends JdbcThinAbstractSelfTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1";

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration(DEFAULT_CACHE_NAME));

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setMarshaller(new BinaryMarshaller());

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
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(1);

        BasicDataSource ds = new BasicDataSource();

        ds.setDriverClassName("org.apache.ignite.IgniteJdbcThinDriver");
        ds.setUrl("jdbc:ignite:thin://localhost");

        try (final Connection conn = ds.getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE TEST (TEST_ID INTEGER PRIMARY KEY, TEST_VAL INTEGER)");
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentUsageConnection() throws Exception {
        final BasicDataSource ds = new BasicDataSource();

        ds.setDriverClassName("org.apache.ignite.IgniteJdbcThinDriver");
        ds.setUrl("jdbc:ignite:thin://localhost");

        final boolean err[] = new boolean[] {false};

        try (final Connection conn = ds.getConnection()) {
            GridTestUtils.runMultiThreaded(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    try {
                        for (int i = 0; i < 100 && !err[0]; ++i) {
                            DatabaseMetaData meta = conn.getMetaData();

                            ResultSet rs = meta.getTables(null, null, "test", null);

                            assert !rs.next();
                        }
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                        err[0] = true;
                    }

                    return null;
                }
            }, 10, "meta-thread");
        }

        assertFalse("Concurrent usage of Ignite connection. See the log.", err[0]);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOneConnectionPerThread() throws Exception {
        final BasicDataSource ds = new BasicDataSource();

        ds.setDriverClassName("org.apache.ignite.IgniteJdbcThinDriver");
        ds.setUrl("jdbc:ignite:thin://localhost");

        final boolean err[] = new boolean[] {false};

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                try (final Connection conn = ds.getConnection()) {
                    try {
                        for (int i = 0; i < 100; ++i) {
                            DatabaseMetaData meta = conn.getMetaData();

                            ResultSet rs = meta.getTables(null, null, "test", null);

                            assert !rs.next();
                        }
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                        err[0] = true;
                    }

                    return null;
                }
            }
        }, 2, "meta-thread");

        assertFalse("Concurrent usage of Ignite connection. See the log.", err[0]);
    }
}