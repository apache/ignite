/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test cases for checking cancellation rebalancing process if some events occurs.
 */
@RunWith(JUnit4.class)
public class GridCacheRebalancingCancelTest extends GridCommonAbstractTest {
    /** */
    private static final String DHT_PARTITIONED_CACHE = "cacheP";

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration dfltCfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)dfltCfg.getDiscoverySpi()).setIpFinder(ipFinder);

        dfltCfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return dfltCfg;
    }

    /**
     * Test rebalance not cancelled when client node join to cluster.
     *
     * @throws Exception Exception.
     */
    @Test
    public void testClientNodeJoinAtRebalancing() throws Exception {
        final IgniteEx ignite0 = startGrid(0);

        IgniteCache<Integer, Integer> cache = ignite0.createCache(
            new CacheConfiguration<Integer, Integer>(DHT_PARTITIONED_CACHE)
                .setCacheMode(CacheMode.PARTITIONED)
                .setRebalanceMode(CacheRebalanceMode.ASYNC)
                .setBackups(1)
                .setRebalanceOrder(2)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setAffinity(new RendezvousAffinityFunction(false)));

        for (int i = 0; i < 2048; i++)
            cache.put(i, i);

        TestRecordingCommunicationSpi.spi(ignite0)
            .blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message msg) {
                    return (msg instanceof GridDhtPartitionSupplyMessage)
                        && ((GridCacheGroupIdMessage)msg).groupId() == groupIdForCache(ignite0, DHT_PARTITIONED_CACHE);
                }
            });

        final IgniteEx ignite1 = startGrid(1);

        TestRecordingCommunicationSpi.spi(ignite0).waitForBlocked();

        GridDhtPartitionDemander.RebalanceFuture fut = (GridDhtPartitionDemander.RebalanceFuture)ignite1.context().
            cache().internalCache(DHT_PARTITIONED_CACHE).preloader().rebalanceFuture();

        String igniteClntName = getTestIgniteInstanceName(2);

        startGrid(igniteClntName, optimize(getConfiguration(igniteClntName).setClientMode(true)));

        // Resend delayed rebalance messages.
        TestRecordingCommunicationSpi.spi(ignite0).stopBlock(true);

        awaitPartitionMapExchange();

        // Previous rebalance future should not be cancelled.
        assertTrue(fut.result());
    }
}
