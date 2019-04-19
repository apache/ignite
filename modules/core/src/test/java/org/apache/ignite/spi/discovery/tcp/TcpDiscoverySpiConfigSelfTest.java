/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.discovery.tcp;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractConfigTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.junit.Test;

/**
 *
 */
@GridSpiTest(spi = TcpDiscoverySpi.class, group = "Discovery SPI")
public class TcpDiscoverySpiConfigSelfTest extends GridSpiAbstractConfigTest<TcpDiscoverySpi> {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNegativeConfig() throws Exception {
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "ipFinder", null);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "ipFinderCleanFrequency", 0);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "localPort", 1023);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "localPortRange", 0);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "networkTimeout", 0);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "socketTimeout", 0);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "ackTimeout", 0);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "maxAckTimeout", 0);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "reconnectCount", 0);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "threadPriority", -1);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "statisticsPrintFrequency", 0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalPortRange() throws Exception {
        try {
            IgniteConfiguration cfg = getConfiguration();

            TcpDiscoverySpi spi = new TcpDiscoverySpi();

            spi.setIpFinder(new TcpDiscoveryVmIpFinder(true));
            spi.setLocalPortRange(0);
            cfg.setDiscoverySpi(spi);

            startGrid(cfg.getIgniteInstanceName(), cfg);
        }
        finally {
            stopAllGrids();
        }
    }
}
