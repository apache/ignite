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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test checks what happens when the rebalance chain is breaking of two parts.
 */
public class BreakRebalanceChainTest extends GridCommonAbstractTest {
    /** Node name suffex. Used for {@link CustomNodeFilter}. */
    public static final String FILTERED_NODE_SUFFIX = "_filtered";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()))
            .setCacheConfiguration(
                new CacheConfiguration(DEFAULT_CACHE_NAME + 1)
                    .setAffinity(new RendezvousAffinityFunction(false, 15))
                    .setNodeFilter(new CustomNodeFilter())
                    .setRebalanceOrder(1)
                    .setBackups(1),
                new CacheConfiguration(DEFAULT_CACHE_NAME + 2)
                    .setAffinity(new RendezvousAffinityFunction(false, 15))
                    .setRebalanceOrder(2)
                    .setBackups(1),
                new CacheConfiguration(DEFAULT_CACHE_NAME + 3)
                    .setAffinity(new RendezvousAffinityFunction(false, 15))
                    .setNodeFilter(new CustomNodeFilter())
                    .setRebalanceOrder(3)
                    .setBackups(1));
    }

    /**
     * Custom node filter. It filters all node that name contains a {@link BreakRebalanceChainTest.FILTERED_NODE_SUFFIX}.
     */
    private static class CustomNodeFilter implements IgnitePredicate<ClusterNode> {
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return !node.consistentId().toString().contains(FILTERED_NODE_SUFFIX);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void test() throws Exception {
        startGrids(2);

        awaitPartitionMapExchange();

        IgniteConfiguration cfg = optimize(getConfiguration(getTestIgniteInstanceName(2)));

        TestRecordingCommunicationSpi communicationSpi = (TestRecordingCommunicationSpi)cfg.getCommunicationSpi();

        ConcurrentHashMap<String, Long> rebalancingCaches = new ConcurrentHashMap<>();

        communicationSpi.blockMessages((node, msg) -> {
            if (msg instanceof GridDhtPartitionDemandMessage) {
                GridDhtPartitionDemandMessage demandMsg = (GridDhtPartitionDemandMessage)msg;

                long rebId = U.field(demandMsg, "rebalanceId");

                if (demandMsg.groupId() == CU.cacheId(DEFAULT_CACHE_NAME + 1)) {
                    rebalancingCaches.put(DEFAULT_CACHE_NAME + 1, rebId);

                    return true;
                }
                else if (demandMsg.groupId() == CU.cacheId(DEFAULT_CACHE_NAME + 2)) {
                    rebalancingCaches.put(DEFAULT_CACHE_NAME + 2, rebId);

                    return true;
                }
                else if (demandMsg.groupId() == CU.cacheId(DEFAULT_CACHE_NAME + 3)) {
                    rebalancingCaches.put(DEFAULT_CACHE_NAME + 3, rebId);

                    return true;
                }
            }

            return false;
        });

        startGrid(cfg);

        communicationSpi.waitForBlocked();

        assertEquals("Several parallel rebalace detected.", blockedDemand(rebalancingCaches), 1);

        IgniteEx filteredNode = startGrid(getTestIgniteInstanceName(3) + FILTERED_NODE_SUFFIX);

        IgniteInternalFuture<Boolean>[] futs = getAllRebalanceFutures(filteredNode);

        for (IgniteInternalFuture fut : futs)
            fut.get(10_000);

        assertEquals("Several parallel rebalace detected.", blockedDemand(rebalancingCaches), 1);

        communicationSpi.stopBlock();

        awaitPartitionMapExchange();
    }

    /**
     * @param rebalancingCaches Map of blocked demande messages for caches.
     * @return Count of blocked messages.
     */
    private int blockedDemand(ConcurrentHashMap<String, Long> rebalancingCaches) {
        int mesages = 0;

        for (Map.Entry<String, Long> entry : rebalancingCaches.entrySet()) {
            if (entry.getValue() > 0) {
                mesages++;

                log.info("Demand for partitions on cache " + entry.getKey() + " rebalance id " + entry.getValue());
            }
        }

        return mesages;
    }

    /**
     * Finds all existed rebalance future by all cache for Ignite's instance specified.
     *
     * @param ignite Ignite.
     * @return Array of rebelance futures.
     */
    private IgniteInternalFuture<Boolean>[] getAllRebalanceFutures(IgniteEx ignite) {
        IgniteInternalFuture<Boolean>[] futs = new IgniteInternalFuture[ignite.cacheNames().size()];

        int i = 0;

        for (String cache : ignite.cacheNames()) {
            futs[i] = ignite.context().cache().cacheGroup(CU.cacheId(cache)).preloader().rebalanceFuture();

            i++;
        }
        return futs;
    }
}
