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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.OdbcConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

/**
 * Statement test
 */
public class JdbcStatementSelfTest extends GridCommonAbstractTest {

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
    public void testExecuteQuery() throws Exception {
        Statement stmt = getStatement();

        final String sqlText = "select * from test";

        // TODO: need real data
        ResultSet resultSet = stmt.executeQuery(sqlText);

        assertNotNull(resultSet);

        stmt.setQueryTimeout(1);


        // TODO: cannot be called on PreparedStatement
        //
    }

    private Statement getStatement() throws Exception {
        final Connection conn = DriverManager.getConnection(URL_PREFIX + HOST);

        Statement stmt = conn.createStatement();

        return stmt;
    }
}
