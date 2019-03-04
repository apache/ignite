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

package org.apache.ignite.internal.processors.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConnectorConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

/**
 * SQL connector configuration validation tests.
 */
@SuppressWarnings("deprecation")
public class SqlConnectorConfigurationValidationSelfTest extends AbstractIndexingCommonTest {
    /** Node index generator. */
    private static final AtomicInteger NODE_IDX_GEN = new AtomicInteger();

    /** Cache name. */
    private static final String CACHE_NAME = "CACHE";

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Test host.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDefault() throws Exception {
        check(new SqlConnectorConfiguration(), true);
        assertJdbc(null, SqlConnectorConfiguration.DFLT_PORT);
    }

    /**
     * Test host.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testHost() throws Exception {
        check(new SqlConnectorConfiguration().setHost("126.0.0.1"), false);

        check(new SqlConnectorConfiguration().setHost("127.0.0.1"), true);
        assertJdbc("127.0.0.1", SqlConnectorConfiguration.DFLT_PORT);

        check(new SqlConnectorConfiguration().setHost("0.0.0.0"), true);
        assertJdbc("0.0.0.0", SqlConnectorConfiguration.DFLT_PORT + 1);

    }

    /**
     * Test port.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPort() throws Exception {
        check(new SqlConnectorConfiguration().setPort(-1), false);
        check(new SqlConnectorConfiguration().setPort(0), false);
        check(new SqlConnectorConfiguration().setPort(512), false);
        check(new SqlConnectorConfiguration().setPort(65536), false);

        check(new SqlConnectorConfiguration().setPort(SqlConnectorConfiguration.DFLT_PORT), true);
        assertJdbc(null, SqlConnectorConfiguration.DFLT_PORT);

        check(new SqlConnectorConfiguration().setPort(SqlConnectorConfiguration.DFLT_PORT + 200), true);
        assertJdbc(null, SqlConnectorConfiguration.DFLT_PORT + 200);
    }


    /**
     * Test port.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPortRange() throws Exception {
        check(new SqlConnectorConfiguration().setPortRange(-1), false);

        check(new SqlConnectorConfiguration().setPortRange(0), true);
        assertJdbc(null, SqlConnectorConfiguration.DFLT_PORT);

        check(new SqlConnectorConfiguration().setPortRange(10), true);
        assertJdbc(null, SqlConnectorConfiguration.DFLT_PORT + 1);
    }

    /**
     * Test socket buffers.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSocketBuffers() throws Exception {
        check(new SqlConnectorConfiguration().setSocketSendBufferSize(-4 * 1024), false);
        check(new SqlConnectorConfiguration().setSocketReceiveBufferSize(-4 * 1024), false);

        check(new SqlConnectorConfiguration().setSocketSendBufferSize(4 * 1024), true);
        assertJdbc(null, SqlConnectorConfiguration.DFLT_PORT);

        check(new SqlConnectorConfiguration().setSocketReceiveBufferSize(4 * 1024), true);
        assertJdbc(null, SqlConnectorConfiguration.DFLT_PORT + 1);
    }

    /**
     * Test max open cursors per connection.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMaxOpenCusrorsPerConnection() throws Exception {
        check(new SqlConnectorConfiguration().setMaxOpenCursorsPerConnection(-1), false);

        check(new SqlConnectorConfiguration().setMaxOpenCursorsPerConnection(0), true);
        assertJdbc(null, SqlConnectorConfiguration.DFLT_PORT);

        check(new SqlConnectorConfiguration().setMaxOpenCursorsPerConnection(100), true);
        assertJdbc(null, SqlConnectorConfiguration.DFLT_PORT + 1);
    }

    /**
     * Test thread pool size.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testThreadPoolSize() throws Exception {
        check(new SqlConnectorConfiguration().setThreadPoolSize(0), false);
        check(new SqlConnectorConfiguration().setThreadPoolSize(-1), false);

        check(new SqlConnectorConfiguration().setThreadPoolSize(4), true);
        assertJdbc(null, SqlConnectorConfiguration.DFLT_PORT);
    }

    /**
     * Perform check.
     *
     * @param sqlCfg SQL configuration.
     * @param success Success flag. * @throws Exception If failed.
     */
    @SuppressWarnings({"unchecked"})
    private void check(SqlConnectorConfiguration sqlCfg, boolean success) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration();

        cfg.setIgniteInstanceName(SqlConnectorConfigurationValidationSelfTest.class.getName() + "-" +
            NODE_IDX_GEN.incrementAndGet());

        cfg.setLocalHost("127.0.0.1");
        cfg.setSqlConnectorConfiguration(sqlCfg);
        cfg.setMarshaller(new BinaryMarshaller());

        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        spi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(spi);

        CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME)
            .setIndexedTypes(SqlConnectorKey.class, SqlConnectorValue.class);

        cfg.setCacheConfiguration(ccfg);

        if (success)
            startGrid(cfg.getIgniteInstanceName(), cfg);
        else {
            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    startGrid(cfg.getIgniteInstanceName(), cfg);

                    return null;
                }
            }, IgniteException.class, null);
        }
    }

    /**
     * Make sure that JDBC connection is possible at the given host and port.
     *
     * @param host Host.
     * @param port Port.
     * @throws Exception If failed.
     */
    private void assertJdbc(@Nullable String host, int port) throws Exception {
        if (host == null)
            host = "127.0.0.1";

        String connStr = "jdbc:ignite:thin://" + host + ":" + port;

        try (Connection conn = DriverManager.getConnection(connStr)) {
            conn.setSchema(CACHE_NAME);

            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT 1");

                assertTrue(rs.next());

                Assert.assertEquals(1, rs.getInt(1));
            }
        }
    }

    /**
     * Key class.
     */
    private static class SqlConnectorKey {
        @QuerySqlField
        public int key;
    }

    /**
     * Value class.
     */
    private static class SqlConnectorValue {
        @QuerySqlField
        public int val;
    }
}
