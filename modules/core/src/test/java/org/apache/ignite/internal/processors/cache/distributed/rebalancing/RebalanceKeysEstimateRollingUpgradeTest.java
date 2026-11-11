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

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.rollingupgrade.AbstractRollingUpgradeTest;
import org.apache.ignite.internal.util.typedef.G;
import org.junit.Test;

import static org.apache.ignite.internal.processors.rollingupgrade.feature.CoreFeatureRegistry.PARTITION_SIZES_IN_SUPPLY_FEATURE;

/**
 * Until {@code PARTITION_SIZES_IN_SUPPLY_FEATURE} is active the rebalance keys estimate still comes from the partition
 * sizes broadcast by the coordinator on exchange; after activation the exchange carries none and the estimate is intact.
 */
public class RebalanceKeysEstimateRollingUpgradeTest extends AbstractRollingUpgradeTest {
    /** */
    private static final int KEYS = 500;

    /** */
    private static final String OLD_VER = "2.18.0";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName, String ver) throws Exception {
        return super.getConfiguration(igniteInstanceName, ver)
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setCacheMode(CacheMode.REPLICATED)
                .setStatisticsEnabled(true)
                .setAffinity(new RendezvousAffinityFunction(false, 16)));
    }

    /** */
    @Test
    public void testEstimateSourceSwitchesOnActivation() throws Exception {
        startGrid(0, OLD_VER);
        startGrid(1, OLD_VER);

        assertFalse(isFeatureActive(grid(0), PARTITION_SIZES_IN_SUPPLY_FEATURE));

        for (int i = 0; i < KEYS; i++)
            grid(0).cache(DEFAULT_CACHE_NAME).put(i, i);

        checkJoinRebalance(2, OLD_VER, true);

        ru(grid(0)).enableVersionUpgrade();

        for (int i = 0; i < 3; i++) {
            upgradeNodeVersion(i, OLD_VER, MASTER_VER_REF);

            // In-memory data: the restarted node must own it again before the next one restarts.
            awaitPartitionMapExchange();
        }

        // Every node runs the new version, the feature stays inactive until the cluster version is finalized.
        checkJoinRebalance(3, MASTER_VER_REF, true);

        finalizeClusterVersion(0, MASTER_VER_REF);

        checkJoinRebalance(4, MASTER_VER_REF, false);
    }

    /**
     * @param idx Index of the node to start.
     * @param ver Version of the node to start.
     * @param sizesInExchange {@code True} if the exchange full message must carry the partition sizes.
     */
    private void checkJoinRebalance(int idx, String ver, boolean sizesInExchange) throws Exception {
        for (Ignite ignite : G.allGrids())
            TestRecordingCommunicationSpi.spi(ignite).record(GridDhtPartitionsFullMessage.class);

        startGrid(idx, ver);

        awaitPartitionMapExchange();

        assertEquals(KEYS, grid(idx).cache(DEFAULT_CACHE_NAME).localMetrics().getEstimatedRebalancingKeys());

        List<GridDhtPartitionsFullMessage> exchMsgs = G.allGrids().stream()
            .flatMap(ignite -> TestRecordingCommunicationSpi.spi(ignite).recordedMessages(true).stream())
            .map(GridDhtPartitionsFullMessage.class::cast)
            .filter(msg -> msg.exchangeId() != null)
            .collect(Collectors.toList());

        assertFalse(exchMsgs.isEmpty());

        for (GridDhtPartitionsFullMessage msg : exchMsgs)
            assertEquals(sizesInExchange, msg.partitionSizes() != null);
    }
}
