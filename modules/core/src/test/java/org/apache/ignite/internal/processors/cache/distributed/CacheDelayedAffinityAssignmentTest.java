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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.fair.FairAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessageV2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class CacheDelayedAffinityAssignmentTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES = 4;

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        cfg.setCommunicationSpi(commSpi);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

       // startGridsMultiThreaded(NODES);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    static class CachePredicate implements IgnitePredicate<ClusterNode> {
        @Override public boolean apply(ClusterNode clusterNode) {
            String name = clusterNode.attribute(IgniteNodeAttributes.ATTR_GRID_NAME).toString();

            return !name.endsWith("0");
        }
    }
    /**
     * @throws Exception If failed.
     */
    public void testDelayedAffinityAssignment1() throws Exception {
        startGrid(0);

        CacheConfiguration ccfg = new CacheConfiguration();
        //ccfg.setNodeFilter(new CachePredicate());
        ccfg.setAffinity(new FairAffinityFunction());

        ignite(0).createCache(ccfg);

        startGrid(1);

        IgniteCache cache = ignite(1).cache(null);

        for (int i = 0 ; i < 100; i++) {
            cache.put(1, 1);

            assertNotNull(cache.get(1));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDelayedAffinityAssignment2() throws Exception {
        startGrid(0);

        CacheConfiguration ccfg = new CacheConfiguration();
        ccfg.setNodeFilter(new CachePredicate());

        ignite(0).createCache(ccfg);

        startGrid(1);

//        client = true;
//
//        Ignite client = startGrid(2);
//
//        IgniteCache cache = client.cache(null);
//
//        for (int i = 0; i < 100; i++) {
//            cache.put(1, 1);
//
//            assertNotNull(cache.get(1));
//        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDelayedAffinityAssignment3() throws Exception {
        startGrid(0);

        ignite(0).createCache(new CacheConfiguration<>());

        startGrid(1);

        client = true;

        Ignite client = startGrid(2);

        IgniteCache cache = client.cache(null);

        for (int i = 0 ; i < 100; i++) {
            cache.put(1, 1);

            assertNotNull(cache.get(1));
        }


        //ignite(0).createCache(new CacheConfiguration<Object, Object>());
//        final CacheConfiguration ccfg = new CacheConfiguration();
//
//        ignite(0).createCache(ccfg);
//
//        for (int i = 0; i < NODES; i++) {
//            Ignite ignite = ignite(i);
//
//            TestRecordingCommunicationSpi spi =
//                (TestRecordingCommunicationSpi)ignite.configuration().getCommunicationSpi();
//
//            spi.blockMessages(new IgnitePredicate<GridIoMessage>() {
//                @Override public boolean apply(GridIoMessage ioMsg) {
//                    if (!ioMsg.message().getClass().equals(GridDhtPartitionSupplyMessageV2.class))
//                        return false;
//
//                    GridDhtPartitionSupplyMessageV2 msg = (GridDhtPartitionSupplyMessageV2)ioMsg.message();
//
//                    return msg.cacheId() == CU.cacheId(ccfg.getName());
//                }
//            });
//        }
//
//        startGrid(NODES);
//
//        Ignite ignite = ignite(0);
//
//        Affinity aff = ignite.affinity(ccfg.getName());
    }
}
