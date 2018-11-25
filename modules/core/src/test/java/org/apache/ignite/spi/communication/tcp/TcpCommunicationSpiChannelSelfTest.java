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

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class TcpCommunicationSpiChannelSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Nodes count. */
    private static final int NODES_CNT = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

//        cfg.setFailureDetectionTimeout(1000);

        TestCommunicationSpi spi = new TestCommunicationSpi();

//        spi.setIdleConnectionTimeout(100);
//        spi.setSharedMemoryPort(-1);

        TcpDiscoverySpi discoSpi = (TcpDiscoverySpi) cfg.getDiscoverySpi();
        discoSpi.setIpFinder(IP_FINDER);

        cfg.setCommunicationSpi(spi);
        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testChannelCreationOnDemand() throws Exception {
        startGrids(NODES_CNT);

//        final CountDownLatch latch = new CountDownLatch(1);

//        grid(0).events().localListen(new IgnitePredicate<Event>() {
//            @Override
//            public boolean apply(Event event) {
//                latch.countDown();
//
//                return true;
//            }
//        }, EVT_NODE_FAILED);

        TcpCommunicationSpi commSpi = (TcpCommunicationSpi) grid(0).configuration().getCommunicationSpi();

        commSpi.getOrCreateChannel(grid(1).localNode());
    }

    /**
     *
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override protected GridCommunicationClient createTcpClient(ClusterNode node, int connIdx)
            throws IgniteCheckedException {

//            if (pred.apply(getLocalNode(), node)) {
//                Map<String, Object> attrs = new HashMap<>(node.attributes());
//
//                attrs.put(createAttributeName(ATTR_ADDRS), Collections.singleton("127.0.0.1"));
//                attrs.put(createAttributeName(ATTR_PORT), 47200);
//                attrs.put(createAttributeName(ATTR_EXT_ADDRS), Collections.emptyList());
//                attrs.put(createAttributeName(ATTR_HOST_NAMES), Collections.emptyList());
//
//                ((TcpDiscoveryNode)node).setAttributes(attrs);
//            }

            return super.createTcpClient(node, connIdx);
        }

        /**
         * @param name Name.
         */
        private String createAttributeName(String name) {
            return getClass().getSimpleName() + '.' + name;
        }
    }
}
