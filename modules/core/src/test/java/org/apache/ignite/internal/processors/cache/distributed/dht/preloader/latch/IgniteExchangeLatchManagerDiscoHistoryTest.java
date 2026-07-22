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
package org.apache.ignite.internal.processors.cache.distributed.dht.preloader.latch;


import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TestTcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISCOVERY_HISTORY_SIZE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/** */
public class IgniteExchangeLatchManagerDiscoHistoryTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoveryIpFinder ipFinder = ((TcpDiscoverySpi)cfg.getDiscoverySpi()).getIpFinder();

        int topHistSize = 2;

        return cfg.setDiscoverySpi(new CustomTcpDiscoverySpi(topHistSize).setIpFinder(ipFinder));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DISCOVERY_HISTORY_SIZE, value = "2")
    public void testProperException2() throws Exception {
        IgniteEx srv = startGrids(3);

        awaitPartitionMapExchange();

        assertThrows(
            null,
            () -> srv.context().cache().context().exchange().latch().aliveNodesForTopologyVer(new AffinityTopologyVersion(1, 0)),
            IgniteException.class,
            "Consider increasing IGNITE_DISCOVERY_HISTORY_SIZE property. Current value is " + 2
        );
    }

    /** */
    private static class CustomTcpDiscoverySpi extends TestTcpDiscoverySpi {
        /** */
        CustomTcpDiscoverySpi(int topHistSize) {
            this.topHistSize = topHistSize;
        }
    }
}