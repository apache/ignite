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

package org.apache.ignite.ml.dataset.impl.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.dataset.primitive.data.SimpleDatasetData;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests for {@link CacheBasedDataset}.
 */
public class CacheBasedDatasetTest extends GridCommonAbstractTest {
    /** Number of nodes in grid. */
    private static final int NODE_COUNT = 4;

    /** Ignite instance. */
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 1; i <= NODE_COUNT; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() {
        /* Grid instance. */
        ignite = grid(NODE_COUNT);
        ignite.configuration().setPeerClassLoadingEnabled(true);
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
    }

    /**
     * Tests that partitions of the upstream cache and the partition {@code context} cache are reserved during
     * computations on dataset. Reservation means that partitions won't be unloaded from the node before computation is
     * completed.
     */
    @Test
    public void testPartitionExchangeDuringComputeCall() {
        int partitions = 4;

        IgniteCache<Integer, String> upstreamCache = generateTestData(4, 0);

        CacheBasedDatasetBuilder<Integer, String> builder = new CacheBasedDatasetBuilder<>(ignite, upstreamCache);

        CacheBasedDataset<Integer, String, Long, SimpleDatasetData> dataset = builder.build(
            TestUtils.testEnvBuilder(),
            (env, upstream, upstreamSize) -> upstreamSize,
            (env, upstream, upstreamSize, ctx) -> new SimpleDatasetData(new double[0], 0),
            TestUtils.testEnvBuilder().buildForTrainer()
        );

        assertEquals("Upstream cache name from dataset",
            upstreamCache.getName(), dataset.getUpstreamCache().getName());

        assertTrue("Before computation all partitions should not be reserved",
            areAllPartitionsNotReserved(upstreamCache.getName(), dataset.getDatasetCache().getName()));

        UUID numOfStartedComputationsId = UUID.randomUUID();
        IgniteAtomicLong numOfStartedComputations = ignite.atomicLong(numOfStartedComputationsId.toString(), 0, true);

        UUID computationsLockId = UUID.randomUUID();
        IgniteLock computationsLock = ignite.reentrantLock(computationsLockId.toString(), false, true, true);

        // lock computations lock to stop computations in the middle
        computationsLock.lock();

        try {
            new Thread(() -> dataset.compute((data, partIndex) -> {
                // track number of started computations
                ignite.atomicLong(numOfStartedComputationsId.toString(), 0, false).incrementAndGet();
                ignite.reentrantLock(computationsLockId.toString(), false, true, false).lock();
                ignite.reentrantLock(computationsLockId.toString(), false, true, false).unlock();
            })).start();
            // wait all computations to start

            while (numOfStartedComputations.get() < partitions) {
            }

            assertTrue("During computation all partitions should be reserved",
                areAllPartitionsReserved(upstreamCache.getName(), dataset.getDatasetCache().getName()));
        }
        finally {
            computationsLock.unlock();
        }

        assertTrue("All partitions should be released",
            areAllPartitionsNotReserved(upstreamCache.getName(), dataset.getDatasetCache().getName()));
    }

    /**
     * Tests that partitions of the upstream cache and the partition {@code context} cache are reserved during
     * computations on dataset. Reservation means that partitions won't be unloaded from the node before computation is
     * completed.
     */
    @Test
    public void testPartitionExchangeDuringComputeWithCtxCall() {
        int partitions = 4;

        IgniteCache<Integer, String> upstreamCache = generateTestData(4, 0);

        CacheBasedDatasetBuilder<Integer, String> builder = new CacheBasedDatasetBuilder<>(ignite, upstreamCache);

        CacheBasedDataset<Integer, String, Long, SimpleDatasetData> dataset = builder.build(
            TestUtils.testEnvBuilder(),
            (env, upstream, upstreamSize) -> upstreamSize,
            (env, upstream, upstreamSize, ctx) -> new SimpleDatasetData(new double[0], 0),
            TestUtils.testEnvBuilder().buildForTrainer()
        );

        assertTrue("Before computation all partitions should not be reserved",
            areAllPartitionsNotReserved(upstreamCache.getName(), dataset.getDatasetCache().getName()));

        UUID numOfStartedComputationsId = UUID.randomUUID();
        IgniteAtomicLong numOfStartedComputations = ignite.atomicLong(numOfStartedComputationsId.toString(), 0, true);

        UUID computationsLockId = UUID.randomUUID();
        IgniteLock computationsLock = ignite.reentrantLock(computationsLockId.toString(), false, true, true);

        // lock computations lock to stop computations in the middle
        computationsLock.lock();

        try {
            new Thread(() -> dataset.computeWithCtx((ctx, data, partIndex) -> {
                // track number of started computations
                ignite.atomicLong(numOfStartedComputationsId.toString(), 0, false).incrementAndGet();
                ignite.reentrantLock(computationsLockId.toString(), false, true, false).lock();
                ignite.reentrantLock(computationsLockId.toString(), false, true, false).unlock();
            })).start();
            // wait all computations to start

            while (numOfStartedComputations.get() < partitions) {
            }

            assertTrue("During computation all partitions should be reserved",
                areAllPartitionsReserved(upstreamCache.getName(), dataset.getDatasetCache().getName()));
        }
        finally {
            computationsLock.unlock();
        }

        assertTrue("All partitions should be released",
            areAllPartitionsNotReserved(upstreamCache.getName(), dataset.getDatasetCache().getName()));
    }

    /**
     * Checks that all partitions of all specified caches are not reserved.
     *
     * @param cacheNames Cache names to be checked.
     * @return {@code true} if all partitions are not reserved, otherwise {@code false}.
     */
    private boolean areAllPartitionsNotReserved(String... cacheNames) {
        return checkAllPartitions(partition -> partition.reservations() == 0, cacheNames);
    }

    /**
     * Checks that all partitions of all specified caches not reserved.
     *
     * @param cacheNames Cache names to be checked.
     * @return {@code true} if all partitions are reserved, otherwise {@code false}.
     */
    private boolean areAllPartitionsReserved(String... cacheNames) {
        return checkAllPartitions(partition -> partition.reservations() != 0, cacheNames);
    }

    /**
     * Checks that all partitions of all specified caches satisfies the given predicate.
     *
     * @param pred Predicate.
     * @param cacheNames Cache names.
     * @return {@code true} if all partitions satisfies the given predicate.
     */
    private boolean checkAllPartitions(IgnitePredicate<GridDhtLocalPartition> pred, String... cacheNames) {
        boolean flag = false;
        long checkingStartTs = System.currentTimeMillis();

        while (!flag && (System.currentTimeMillis() - checkingStartTs) < 30_000) {
            LockSupport.parkNanos(200L * 1000 * 1000);
            flag = true;

            for (String cacheName : cacheNames) {
                IgniteClusterPartitionsState state = IgniteClusterPartitionsState.getCurrentState(cacheName);

                for (IgniteInstancePartitionsState instanceState : state.instances.values())
                    for (GridDhtLocalPartition partition : instanceState.parts)
                        if (partition != null)
                            flag &= pred.apply(partition);
            }
        }

        return flag;
    }

    /**
     * Aggregated data about cache partitions in Ignite cluster.
     */
    private static class IgniteClusterPartitionsState {
        /** */
        private final String cacheName;

        /** */
        private final Map<UUID, IgniteInstancePartitionsState> instances;

        /** */
        static IgniteClusterPartitionsState getCurrentState(String cacheName) {
            Map<UUID, IgniteInstancePartitionsState> instances = new HashMap<>();

            for (Ignite ignite : G.allGrids()) {
                IgniteKernal igniteKernal = (IgniteKernal)ignite;
                IgniteCacheProxy<?, ?> cache = igniteKernal.context().cache().jcache(cacheName);

                GridDhtCacheAdapter<?, ?> dht = dht(cache);

                GridDhtPartitionTopology top = dht.topology();

                AffinityTopologyVersion topVer = dht.context().shared().exchange().readyAffinityVersion();
                List<GridDhtLocalPartition> parts = new ArrayList<>();
                for (int p = 0; p < cache.context().config().getAffinity().partitions(); p++) {
                    GridDhtLocalPartition part = top.localPartition(p, AffinityTopologyVersion.NONE, false);
                    parts.add(part);
                }
                instances.put(ignite.cluster().localNode().id(), new IgniteInstancePartitionsState(topVer, parts));
            }

            return new IgniteClusterPartitionsState(cacheName, instances);
        }

        /** */
        IgniteClusterPartitionsState(String cacheName,
            Map<UUID, IgniteInstancePartitionsState> instances) {
            this.cacheName = cacheName;
            this.instances = instances;
        }

        /** */
        @Override public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("Cache ").append(cacheName).append(" is in following state:").append("\n");
            for (Map.Entry<UUID, IgniteInstancePartitionsState> e : instances.entrySet()) {
                UUID instanceId = e.getKey();
                IgniteInstancePartitionsState instanceState = e.getValue();
                builder.append("\n\t")
                    .append("Node ")
                    .append(instanceId)
                    .append(" with topology version [")
                    .append(instanceState.topVer.topologyVersion())
                    .append(", ")
                    .append(instanceState.topVer.minorTopologyVersion())
                    .append("] contains following partitions:")
                    .append("\n\n");
                builder.append("\t\t---------------------------------------------------------------------------------");
                builder.append("--------------------\n");
                builder.append("\t\t|  ID  |   STATE  |  RELOAD  |  RESERVATIONS  |  SHOULD BE RENTING  |  PRIMARY  |");
                builder.append("  DATA STORE SIZE  |\n");
                builder.append("\t\t---------------------------------------------------------------------------------");
                builder.append("--------------------\n");
                for (GridDhtLocalPartition partition : instanceState.parts)
                    if (partition != null) {
                        builder.append("\t\t")
                            .append(String.format("| %3d  |", partition.id()))
                            .append(String.format(" %7s  |", partition.state()))
                            .append(String.format(" %13s  |", partition.reservations()))
                            .append(String.format(" %8s  |", partition.primary(instanceState.topVer)))
                            .append(String.format(" %16d  |", partition.dataStore().fullSize()))
                            .append("\n");
                        builder.append("\t\t-------------------------------------------------------------------------");
                        builder.append("----------------------------\n");
                    }
            }
            return builder.toString();
        }
    }

    /**
     * Aggregated data about cache partitions in Ignite instance.
     */
    private static class IgniteInstancePartitionsState {
        /** */
        private final AffinityTopologyVersion topVer;

        /** */
        private final List<GridDhtLocalPartition> parts;

        /** */
        IgniteInstancePartitionsState(AffinityTopologyVersion topVer,
            List<GridDhtLocalPartition> parts) {
            this.topVer = topVer;
            this.parts = parts;
        }

        /** */
        public AffinityTopologyVersion getTopVer() {
            return topVer;
        }

        /** */
        public List<GridDhtLocalPartition> getParts() {
            return parts;
        }
    }

    /**
     * Generates Ignite Cache with data for tests.
     *
     * @return Ignite Cache with data for tests.
     */
    private IgniteCache<Integer, String> generateTestData(int partitions, int backups) {
        CacheConfiguration<Integer, String> cacheConfiguration = new CacheConfiguration<>();

        cacheConfiguration.setName(UUID.randomUUID().toString());
        cacheConfiguration.setAffinity(new RendezvousAffinityFunction(false, partitions));
        cacheConfiguration.setBackups(backups);

        IgniteCache<Integer, String> cache = ignite.createCache(cacheConfiguration);

        for (int i = 0; i < 1000; i++)
            cache.put(i, "TEST" + i);

        return cache;
    }
}
