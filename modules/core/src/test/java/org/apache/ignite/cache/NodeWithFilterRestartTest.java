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

package org.apache.ignite.cache;

import java.util.Arrays;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class NodeWithFilterRestartTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (getTestIgniteInstanceName(5).equals(igniteInstanceName))
            cfg.setUserAttributes(F.asMap("FILTER", "true"));

//        if (getTestIgniteInstanceName(3).equals(igniteInstanceName))
//            cfg.setUserAttributes(F.asMap("FILTER", "true"));

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        if (getTestIgniteInstanceName(0).equals(igniteInstanceName)) {
            TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

            commSpi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                /** {@inheritDoc} */
                @Override public boolean apply(ClusterNode node, Message msg) {
                    if (msg instanceof GridDhtPartitionsFullMessage && (node.id().getLeastSignificantBits() & 0xFFFF) == 5) {
                        GridDhtPartitionsFullMessage fullMsg = (GridDhtPartitionsFullMessage)msg;

                        if (fullMsg.exchangeId() != null && fullMsg.topologyVersion().equals(new AffinityTopologyVersion(8, 0))) {
                            info("Going to block message [node=" + node + ", msg=" + msg + ']');

                            return true;
                        }
                    }

                    return false;
                }
            });

            cfg.setCommunicationSpi(commSpi);
        }
        else
            cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testSpecificRestart() throws Exception {
        try {
            startGrids(6);

            {
                CacheConfiguration cfg1 = new CacheConfiguration();
                cfg1.setName("TRANSIENT_JOURNEY_ID");
                cfg1.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
                cfg1.setBackups(1);
                cfg1.setRebalanceMode(CacheRebalanceMode.ASYNC);
                cfg1.setAffinity(new RendezvousAffinityFunction(false, 64));
                cfg1.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
                cfg1.setNodeFilter(new NodeFilter());

                CacheConfiguration cfg2 = new CacheConfiguration();
                cfg2.setName("ENTITY_CONFIG");
                cfg2.setAtomicityMode(CacheAtomicityMode.ATOMIC);
                cfg2.setCacheMode(CacheMode.REPLICATED);
                cfg2.setRebalanceMode(CacheRebalanceMode.ASYNC);
                cfg2.setBackups(0);
                cfg2.setAffinity(new RendezvousAffinityFunction(false, 256));

                grid(0).getOrCreateCaches(Arrays.asList(cfg1, cfg2));
            }

            stopGrid(5, true);

            // Start grid 5 to trigger a local join exchange (node 0 is coordinator).
            // Since the message is blocked, node 5 will not complete the exchange.
            // Fail coordinator, check if the next exchange can be completed.
            // 5 should send single message to the new coordinator 1.
            IgniteInternalFuture<IgniteEx> fut = GridTestUtils.runAsync(() -> startGrid(5));

            // Increase if does not reproduce.
            U.sleep(2000);

            stopGrid(0, true);

            fut.get();

            awaitPartitionMapExchange();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    private static class NodeFilter implements IgnitePredicate<ClusterNode> {
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode clusterNode) {
            return "true".equals(clusterNode.attribute("FILTER"));
        }
    }
}
