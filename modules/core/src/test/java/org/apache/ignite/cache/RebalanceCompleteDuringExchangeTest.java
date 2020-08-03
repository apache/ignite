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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * This test hangs exchange and waits rebalance complete. After partitions rebalance completed exchange will unlock and
 * test will wait ideal assignment.
 */
public class RebalanceCompleteDuringExchangeTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        return super.getConfiguration(name)
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setCacheMode(CacheMode.REPLICATED))
            .setCommunicationSpi(new TestRecordingCommunicationSpi());
    }

    /**
     * Waits ideal assignment for configured cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRebalance() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        IgniteCache cache = ignite0.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 2000; i++)
            cache.put(i, i);

        IgniteEx ignite1 = startNodeAndBlockRebalance(1);

        TestRecordingCommunicationSpi commSpi2 = startNodeAndBlockExchange(2);

        TestRecordingCommunicationSpi commSpi1 = TestRecordingCommunicationSpi.spi(ignite1);

        commSpi1.waitForRecorded();

        info(getTestIgniteInstanceName(1) + " sent Single message to coordinator.");

        commSpi1.recordedMessages(true);

        commSpi1.record(GridDhtPartitionsSingleMessage.class);

        commSpi2.waitForBlocked();

        info("Exchange is waiting Single message from " + getTestIgniteInstanceName(2));

        commSpi1.waitForBlocked();

        info("Rebalance on " + getTestIgniteInstanceName(1) + " was blocked.");

        commSpi1.stopBlock();

        //Test is waiting sent information about rebalance complete.
        commSpi1.waitForRecorded();

        info("Rebalance on " + getTestIgniteInstanceName(1) + " was unblocked and completed.");

        commSpi2.stopBlock();

        awaitPartitionMapExchange();
    }

    /**
     * Starts node and blocks exchange on it.
     *
     * @param nodeNum Number of node.
     * @return Test communication spi.
     * @throws Exception If failed.
     */
    public TestRecordingCommunicationSpi startNodeAndBlockExchange(int nodeNum) throws Exception {
        IgniteConfiguration cfg = optimize(getConfiguration(getTestIgniteInstanceName(nodeNum)));

        TestRecordingCommunicationSpi commSpi = (TestRecordingCommunicationSpi)cfg.getCommunicationSpi();

        commSpi.blockMessages(GridDhtPartitionsSingleMessage.class, getTestIgniteInstanceName(0));

        IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
            try {
                IgniteEx ignite2 = startGrid(cfg);
            }
            catch (Exception e) {
                log.error("Start clustr exception " + e.getMessage(), e);
            }
        });

        return commSpi;
    }

    /**
     * Starts node and blocks rebalance.
     *
     * @param nodeNum Number of node.
     * @throws Exception If failed.
     */
    public IgniteEx startNodeAndBlockRebalance(int nodeNum) throws Exception {
        IgniteConfiguration cfg = optimize(getConfiguration(getTestIgniteInstanceName(nodeNum)));

        TestRecordingCommunicationSpi commSpi = (TestRecordingCommunicationSpi)cfg.getCommunicationSpi();

        commSpi.record((ClusterNode node, Message msg) -> {
            if (msg instanceof GridDhtPartitionsSingleMessage) {
                GridDhtPartitionsSingleMessage singleMessage = (GridDhtPartitionsSingleMessage)msg;

                if (singleMessage.exchangeId() == null)
                    return false;

                return singleMessage.exchangeId().topologyVersion().equals(new AffinityTopologyVersion(3, 0));
            }

            return false;
        });

        commSpi.blockMessages((ClusterNode node, Message msg) -> {
            if (msg instanceof GridDhtPartitionDemandMessage) {
                GridDhtPartitionDemandMessage demandMessage = (GridDhtPartitionDemandMessage)msg;

                return CU.cacheId(DEFAULT_CACHE_NAME) == demandMessage.groupId();
            }

            return false;
        });

        return startGrid(cfg);
    }
}
