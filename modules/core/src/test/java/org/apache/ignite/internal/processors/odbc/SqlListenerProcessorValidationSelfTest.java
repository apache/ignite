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

package org.apache.ignite.internal.processors.odbc;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.OdbcConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * ODBC configuration validation tests.
 */
public class SqlListenerProcessorValidationSelfTest extends GridCommonAbstractTest {
    /** Node index generator. */
    private static final AtomicInteger NODE_IDX_GEN = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Ensure we can start with default configuration.
     *
     * @throws Exception If failed.
     */
    public void testAddressDefault() throws Exception {
        check(new OdbcConfiguration(), true);
    }

    /**
     * Test address where only host is provided.
     *
     * @throws Exception If failed.
     */
    public void testAddressHostOnly() throws Exception {
        check(new OdbcConfiguration().setEndpointAddress("127.0.0.1"), true);
    }

    /**
     * Test address with both host and port.
     *
     * @throws Exception If failed.
     */
    public void testAddressHostAndPort() throws Exception {
        check(new OdbcConfiguration().setEndpointAddress("127.0.0.1:9999"), true);

        // Shouldn't fit into range.
        check(new OdbcConfiguration().setEndpointAddress("127.0.0.1:9999"), false);
    }

    /**
     * Test address with host and port range.
     *
     * @throws Exception If failed.
     */
    public void testAddressHostAndPortRange() throws Exception {
        check(new OdbcConfiguration().setEndpointAddress("127.0.0.1:9999..10000"), true);
        check(new OdbcConfiguration().setEndpointAddress("127.0.0.1:9999..10000"), true);

        // Shouldn't fit into range.
        check(new OdbcConfiguration().setEndpointAddress("127.0.0.1:9999..10000"), false);
    }

    /**
     * Test start with invalid host.
     *
     * @throws Exception If failed.
     */
    public void testAddressInvalidHost() throws Exception {
        check(new OdbcConfiguration().setEndpointAddress("126.0.0.1"), false);
    }

    /**
     * Test start with invalid address format.
     *
     * @throws Exception If failed.
     */
    public void testAddressInvalidFormat() throws Exception {
        check(new OdbcConfiguration().setEndpointAddress("127.0.0.1:"), false);

        check(new OdbcConfiguration().setEndpointAddress("127.0.0.1:0"), false);
        check(new OdbcConfiguration().setEndpointAddress("127.0.0.1:-1"), false);
        check(new OdbcConfiguration().setEndpointAddress("127.0.0.1:111111"), false);

        check(new OdbcConfiguration().setEndpointAddress("127.0.0.1:9999.."), false);
        check(new OdbcConfiguration().setEndpointAddress("127.0.0.1:9999..9998"), false);

        check(new OdbcConfiguration().setEndpointAddress("127.0.0.1:a"), false);
        check(new OdbcConfiguration().setEndpointAddress("127.0.0.1:a.."), false);
        check(new OdbcConfiguration().setEndpointAddress("127.0.0.1:a..b"), false);

        check(new OdbcConfiguration().setEndpointAddress(":9999"), false);
        check(new OdbcConfiguration().setEndpointAddress(":9999..10000"), false);
    }

    /**
     * Test connection parameters: sendBufferSize, receiveBufferSize, connectionTimeout.
     *
     * @throws Exception If failed.
     */
    public void testConnectionParams() throws Exception {
        check(new OdbcConfiguration().setEndpointAddress("127.0.0.1:9998..10000")
            .setSocketSendBufferSize(4 * 1024), true);

        check(new OdbcConfiguration().setEndpointAddress("127.0.0.1:9998..10000")
            .setSocketReceiveBufferSize(4 * 1024), true);

        check(new OdbcConfiguration().setEndpointAddress("127.0.0.1:9998..10000")
            .setSocketSendBufferSize(-64 * 1024), false);

        check(new OdbcConfiguration().setEndpointAddress("127.0.0.1:9998..10000")
            .setSocketReceiveBufferSize(-64 * 1024), false);
    }

    /**
     * Test thread pool size.
     *
     * @throws Exception If failed.
     */
    public void testThreadPoolSize() throws Exception {
        check(new OdbcConfiguration().setThreadPoolSize(0), false);
        check(new OdbcConfiguration().setThreadPoolSize(-1), false);

        check(new OdbcConfiguration().setThreadPoolSize(4), true);
    }

    /**
     * Perform check.
     *
     * @param odbcCfg ODBC configuration.
     * @param success Success flag. * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    private void check(OdbcConfiguration odbcCfg, boolean success) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration();

        cfg.setIgniteInstanceName(SqlListenerProcessorValidationSelfTest.class.getName() + "-" +
            NODE_IDX_GEN.incrementAndGet());

        cfg.setLocalHost("127.0.0.1");
        cfg.setOdbcConfiguration(odbcCfg);
        cfg.setMarshaller(new BinaryMarshaller());

        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        spi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(spi);

        if (success)
            startGrid(cfg.getGridName(), cfg);
        else {
            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    startGrid(cfg.getGridName(), cfg);

                    return null;
                }
            }, IgniteException.class, null);
        }
    }

}
