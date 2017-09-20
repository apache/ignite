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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

/**
 * Base class for complex SQL tests based on JDBC driver.
 */
public class JdbcThinSelectAfterAlterTable extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** JDBC connection. */
    private Connection conn;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration(DEFAULT_CACHE_NAME));

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
    private CacheConfiguration cacheConfiguration(@NotNull String name) throws Exception {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setName(name);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        conn.close();

        // Destroy all SQL caches after test.
        for (String cacheName : grid(0).cacheNames()) {
            DynamicCacheDescriptor cacheDesc = grid(0).context().cache().cacheDescriptor(cacheName);

            if (cacheDesc != null && cacheDesc.sql())
                grid(0).destroyCache0(cacheName, true);
        }

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testSelectAfterAlterTable() throws Exception {
        Statement stmt = conn.createStatement();

        stmt.executeUpdate("CREATE TABLE person (id LONG, name VARCHAR, city_id LONG, PRIMARY KEY (id, city_id))");

        stmt.executeUpdate("INSERT INTO person (id, name, city_id) values (1, 'name_1', 11)");

        stmt.executeQuery("select * from person");

        stmt.executeUpdate("alter table person add field1 varchar;");

        ResultSet rs = stmt.executeQuery("select * from person");

        ResultSetMetaData meta = rs.getMetaData();

        assert meta.getColumnCount() == 4;

        IgnitePredicate newColExists = new IgnitePredicate<ResultSetMetaData>() {
            @Override public boolean apply(ResultSetMetaData meta) {
                try {
                    for (int i = 0; i < meta.getColumnCount(); ++i) {
                        if ("field1".equalsIgnoreCase(meta.getColumnName(i)))
                            return true;
                    }
                }
                catch (SQLException e) {
                    e.printStackTrace(System.err);

                    fail("Unexpected exception");
                }

                return false;
            }
        };

        assert newColExists.apply(meta);
    }
}