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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.util.LinkedHashSet;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * There are two partition have updates in the different checkpoints. This test checks how historical rebalance works in
 * these circumstances.
 */
@WithSystemProperty(key = IgniteSystemProperties.IGNITE_PREFER_WAL_REBALANCE, value = "true")
public class HistoricalRebalanceTwoPartsInDifferentCheckpointsTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override public IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)))
            .setCacheConfiguration(new CacheConfiguration().setName(DEFAULT_CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                .setCacheMode(CacheMode.PARTITIONED)
                .setBackups(1));
    }

    /** {@inheritDoc} */
    @Override public void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override public void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Checks when updates for 0 partition precedes updates for 2 partition in WAL.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testYoungerPartitionUpdateFirst() throws Exception {
        rebalanceTwoPartitions(true);
    }

    /**
     * Checks when updates for 2 partition precedes updates for 0 partition in WAL.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testOlderPartitionUpdateFirst() throws Exception {
        rebalanceTwoPartitions(false);
    }

    /**
     * Checks a historical rebalance for partitions 0 and 2.
     *
     * @param descending When true first updates is for younger partition, false is older one.
     * @throws Exception If failed.
     */
    public void rebalanceTwoPartitions(boolean descending) throws Exception {
        IgniteEx ignite0 = startGrids(2);

        ignite0.cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();

        LinkedHashSet<Integer> partKeys = new LinkedHashSet<>();

        if (descending) {
            partKeys.addAll(partitionKeys(ignite0.cache(DEFAULT_CACHE_NAME), 0, 10, 0));
            partKeys.addAll(partitionKeys(ignite0.cache(DEFAULT_CACHE_NAME), 2, 10, 0));
        }
        else {
            partKeys.addAll(partitionKeys(ignite0.cache(DEFAULT_CACHE_NAME), 2, 10, 0));
            partKeys.addAll(partitionKeys(ignite0.cache(DEFAULT_CACHE_NAME), 0, 10, 0));
        }

        IgniteCache cache = ignite0.cache(DEFAULT_CACHE_NAME);

        partKeys.forEach(key -> cache.put(key, key));

        info("Data preload completed.");

        stopGrid(1);

        awaitPartitionMapExchange();

        info("Node stopped.");

        for (Integer key : partKeys) {
            cache.put(key, key + 1);

            forceCheckpoint();
        }

        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(1));

        AtomicBoolean hasFullRebalance = new AtomicBoolean();
        AtomicBoolean hasHistoricalRebalance = new AtomicBoolean();

        ((TestRecordingCommunicationSpi)cfg.getCommunicationSpi()).record((node, msg) -> {
            if (msg instanceof GridDhtPartitionDemandMessage) {
                GridDhtPartitionDemandMessage demandMsg = (GridDhtPartitionDemandMessage)msg;

                hasFullRebalance.set(demandMsg.partitions().hasFull());
                hasHistoricalRebalance.set(demandMsg.partitions().hasHistorical());
            }

            return false;
        });

        startGrid(cfg);

        awaitPartitionMapExchange();

        info("Node started and rebalance completed.");

        assertFalse(hasFullRebalance.get());

        assertTrue(hasHistoricalRebalance.get());
    }
}
