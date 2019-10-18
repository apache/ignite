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

package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;

/**
 * Tests to check failover scenarios over tombstones.
 */
public class CacheRemoveWithTombstonesFailoverTest extends PartitionsEvictManagerAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        cfg.setConsistentId(gridName);

        cfg.setCommunicationSpi(commSpi);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setInitialSize(256L * 1024 * 1024)
                    .setMaxSize(256L * 1024 * 1024)
                    .setPersistenceEnabled(true)
            )
            .setCheckpointFrequency(1024 * 1024 * 1024)
            .setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(dsCfg);

        cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    /**
     * Test check that tombstones reside in persistent partition will be cleared after node restart.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    public void testTombstonesClearedAfterRestart() throws Exception {
        IgniteEx crd = startGrid(0);

        crd.cluster().active(true);

        final int KEYS = 1024;

        for (int k = 0; k < KEYS; k++)
            crd.cache(DEFAULT_CACHE_NAME).put(k, k);

        blockRebalance(crd);

        IgniteEx node = startGrid(1);

        // Do not run clear tombsones task.
        instrumentEvictionQueue(node, task -> {
            if (task instanceof PartitionsEvictManager.ClearTombstonesTask)
                return null;

            return task;
        });

        resetBaselineTopology();

        TestRecordingCommunicationSpi.spi(crd).waitForBlocked();

        Set<Integer> keysWithTombstone = new HashSet<>();

        // Do removes while rebalance is in progress.
        for (int i = 0; i < KEYS; i += 2) {
            keysWithTombstone.add(i);

            crd.cache(DEFAULT_CACHE_NAME).remove(i);
        }

        final LongMetric tombstoneMetric = node.context().metric().registry(
            MetricUtils.cacheGroupMetricsRegistryName(DEFAULT_CACHE_NAME)).findMetric("Tombstones");

        Assert.assertEquals(keysWithTombstone.size(), tombstoneMetric.value());

        // Resume rebalance.
        TestRecordingCommunicationSpi.spi(crd).stopBlock();

        // Partitions should be in OWNING state.
        awaitPartitionMapExchange();

        // But tombstones removal should be skipped.
        Assert.assertEquals(keysWithTombstone.size(), tombstoneMetric.value());

        // Stop node with tombstones.
        stopGrid(1);

        // Stop coordinator.
        stopGrid(0);

        // Startup node with tombstones in inactive state.
        node = startGrid(1);

        final int grpId = groupIdForCache(node, DEFAULT_CACHE_NAME);

        // Tombstone metrics are unavailable before join to topology, using internal api.
        long tombstonesBeforeActivation = node.context().cache().cacheGroup(grpId).topology().localPartitions()
            .stream().map(part -> part.dataStore().tombstonesCount()).reduce(Long::sum).orElse(0L);

        Assert.assertEquals(keysWithTombstone.size(), tombstonesBeforeActivation);

        crd = startGrid(0);

        crd.cluster().active(true);

        awaitPartitionMapExchange();

        final LongMetric tombstoneMetric1 = node.context().metric().registry(
            MetricUtils.cacheGroupMetricsRegistryName(DEFAULT_CACHE_NAME)).findMetric("Tombstones");

        // Tombstones should be removed after join to topology.
        GridTestUtils.waitForCondition(() -> tombstoneMetric1.value() == 0, 30_000);

        assertEquals(0, tombstoneMetric1.value());
    }

    /**
     *
     */
    private static void blockRebalance(IgniteEx node) {
        final int grpId = groupIdForCache(node, DEFAULT_CACHE_NAME);

        TestRecordingCommunicationSpi.spi(node).blockMessages((node0, msg) ->
            (msg instanceof GridDhtPartitionSupplyMessage)
                && ((GridCacheGroupIdMessage)msg).groupId() == grpId
        );
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration() {
        return new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL)
            .setCacheMode(PARTITIONED)
            .setBackups(1)
            .setRebalanceMode(ASYNC)
            .setReadFromBackup(true)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAffinity(new RendezvousAffinityFunction(false, 64));
    }
}
