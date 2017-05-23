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
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.OdbcConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

/**
 * Connection test.
 */
public class JdbcConnectionSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** URL prefix. */
    private static final String URL_PREFIX = "jdbc:ignite:thin://";

    /** Host. */
    private static final String HOST = "127.0.0.1";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration(DEFAULT_CACHE_NAME));

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setMarshaller(new BinaryMarshaller());

        cfg.setOdbcConfiguration(new OdbcConfiguration());

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
        try {
            Driver drv = DriverManager.getDriver("jdbc:ignite://");

            if (drv != null)
                DriverManager.deregisterDriver(drv);
        } catch (SQLException ignored) {
            // No-op.
        }

        startGridsMultiThreaded(2);

        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testDefaults() throws Exception {
        String url = URL_PREFIX + HOST;

        assert DriverManager.getConnection(url) != null;
        assert DriverManager.getConnection(url + "/") != null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvalidUrls() throws Exception {
        GridTestUtils.assertThrowsAnyCause(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                DriverManager.getConnection(URL_PREFIX + "127.0.0.1:80");

                return null;
            }
        }, SQLException.class, "Failed to connect to Ignite cluster [host=127.0.0.1, port=80]");

        GridTestUtils.assertThrowsAnyCause(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                DriverManager.getConnection("q");

                return null;
            }
        }, SQLException.class, "URL is invalid");

        GridTestUtils.assertThrowsAnyCause(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                DriverManager.getConnection(URL_PREFIX + "127.0.0.1:-1");

                return null;
            }
        }, SQLException.class, "Invalid port:");

        GridTestUtils.assertThrowsAnyCause(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                DriverManager.getConnection(URL_PREFIX + "127.0.0.1:0");

                return null;
            }
        }, SQLException.class, "Invalid port:");

        GridTestUtils.assertThrowsAnyCause(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                DriverManager.getConnection(URL_PREFIX + "127.0.0.1:100000");

                return null;
            }
        }, SQLException.class, "Invalid port:");

        GridTestUtils.assertThrowsAnyCause(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                DriverManager.getConnection(URL_PREFIX + "     :10000");

                return null;
            }
        }, SQLException.class, "Host name is empty");
    }

    /**
     * @throws Exception If failed.
     */
    public void testClose() throws Exception {
        String url = URL_PREFIX + HOST;

        final Connection conn = DriverManager.getConnection(url);

        assert conn != null;
        assert !conn.isClosed();

        conn.close();

        assert conn.isClosed();

        assert !conn.isValid(2): "Connection must be closed";

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    conn.isValid(-2);

                    return null;
                }
            },
            SQLException.class,
            "Invalid timeout"
        );
    }
}