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

package org.apache.ignite.spi.communication.tcp;

import java.net.NetworkInterface;
import java.util.Collections;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests for communication over discovery feature (inverse communication request).
 */
public class GridSandboxedClientWithoutNetworkTest extends GridCommonAbstractTest {
    /** */
    private int locPort;

    /** */
    private boolean useAnyLocAddress;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        locPort = TcpCommunicationSpi.DFLT_PORT;
        useAnyLocAddress = false;
        IgniteUtils.INTERFACE_SUPPLIER = NetworkInterface::getNetworkInterfaces;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        IgniteUtils.INTERFACE_SUPPLIER = NetworkInterface::getNetworkInterfaces;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setFailureDetectionTimeout(8_000);

        TcpCommunicationSpi communicationSpi = new TcpCommunicationSpi()
                .setLocalPort(locPort);

        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();

        if (useAnyLocAddress) {
            communicationSpi.setLocalAddress("0.0.0.0");
            discoverySpi.setLocalAddress("0.0.0.0");
        }

        cfg.setCommunicationSpi(communicationSpi);
        cfg.setDiscoverySpi(discoverySpi);

        return cfg;
    }

    /**
     * Test that you can't send anything from client to another client that has "-1" local port.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNodeWithoutNetwork() throws Exception {
        IgniteEx srv = startGrid(0);

        IgniteUtils.INTERFACE_SUPPLIER = Collections::emptyEnumeration;

        locPort = -1;

        useAnyLocAddress = true;

        IgniteEx client1 = startClientGrid(1);

        IgniteCache<String, String> testCache = client1.getOrCreateCache("test");

        testCache.put("test", "test");

        assertEquals("test", srv.getOrCreateCache("test").get("test"));
    }
}
