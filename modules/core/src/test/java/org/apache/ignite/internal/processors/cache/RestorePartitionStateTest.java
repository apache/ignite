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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopologyImpl;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.processors.cache.GridCacheProcessor.mergeTopProcessingPartitions;
import static org.apache.ignite.internal.processors.cache.GridCacheProcessor.topProcessingPartitions;

/**
 * Class for testing the restoration of the status of partitions.
 */
public class RestorePartitionStateTest extends GridCommonAbstractTest {
    /** Timeout for displaying the progress of restoring the status of partitions. */
    @Nullable private Long timeoutOutputRestoreProgress;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
        cleanPersistenceDir();

        if (timeoutOutputRestoreProgress != null) {
            long val = GridCacheProcessor.TIMEOUT_OUTPUT_RESTORE_PARTITION_STATE_PROGRESS;

            GridCacheProcessor.TIMEOUT_OUTPUT_RESTORE_PARTITION_STATE_PROGRESS = timeoutOutputRestoreProgress;

            timeoutOutputRestoreProgress = val;
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
        cleanPersistenceDir();

        if (timeoutOutputRestoreProgress != null)
            GridCacheProcessor.TIMEOUT_OUTPUT_RESTORE_PARTITION_STATE_PROGRESS = timeoutOutputRestoreProgress;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(
                new CacheConfiguration<>(DEFAULT_CACHE_NAME + "0")
                    .setAffinity(new RendezvousAffinityFunction(false, 32)),
                new CacheConfiguration<>(DEFAULT_CACHE_NAME + "1")
                    .setAffinity(new RendezvousAffinityFunction(false, 32)),
                new CacheConfiguration<>(DEFAULT_CACHE_NAME + "3")
                    .setAffinity(new RendezvousAffinityFunction(false, 32))
            )
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            );
    }

    @Test
    public void test() throws Exception {
        timeoutOutputRestoreProgress = 50L;

        IgniteEx n = startGrid(0);

        n.cluster().state(ClusterState.ACTIVE);
        awaitPartitionMapExchange();

        GridCacheProcessor cacheProcessor = n.context().cache();

        Map<Integer, Long> processed1 = IntStream.range(0, 10).boxed().collect(toMap(identity(), p -> 10L + ThreadLocalRandom.current().nextLong(10)));
        Map<Integer, Long> processed2 = IntStream.range(0, 10).boxed().collect(toMap(identity(), p -> 10L + ThreadLocalRandom.current().nextLong(10)));

        ((GridCacheDatabaseSharedManager)cacheProcessor.context().database()).enableCheckpoints(false).get(getTestTimeout());

        for (IgniteInternalCache cache : cacheProcessor.caches()) {
            for (int i = 0; i < 10_000; i++)
                cache.put(i, cache.name() + i);
        }

        stopAllGrids();

        LogListener beforeRestore = LogListener.matches(logStr -> {
            if (logStr.startsWith("Restoring partition state for local groups.")) {
                for (CacheGroupContext grp : cacheProcessor.cacheGroups()) {
                    ((GridDhtPartitionTopologyImpl)grp.topology()).partitionFactory((ctx, grp1, id, recovery) -> {
                        if (recovery) {
                            try {
                                U.sleep(15);
                            }
                            catch (IgniteInterruptedCheckedException e) {
                                throw new IgniteException(e);
                            }
                        }

                        return new GridDhtLocalPartition(ctx, grp1, id, recovery);
                    });
                }

                return true;
            }

            return false;
        }).build();

        startGrid(0, (UnaryOperator<IgniteConfiguration>)cfg -> {
            cfg.setGridLogger(new ListeningTestLogger(cfg.getGridLogger(), beforeRestore));

            return cfg;
        });


        System.out.println();
    }

    @Test
    public void test0() {
        for (int i = 0; i < 1_000; i++) {
            Map<Integer, Long> processed0 = generateProcessedPartitions(32);
            Map<Integer, Long> processed1 = generateProcessedPartitions(32);

            NavigableMap<Long, List<Integer>> expTop0 = expTopProcessingPartitions(processed0, 5);
            NavigableMap<Long, List<Integer>> expTop1 = expTopProcessingPartitions(processed1, 5);

            NavigableMap<Long, List<Integer>> actTop0 = topProcessingPartitions(processed0, 5, identity());
            NavigableMap<Long, List<Integer>> actTop1 = topProcessingPartitions(processed1, 5, identity());

            assertEquals(expTop0, actTop0);
            assertEquals(expTop1, actTop1);

            NavigableMap<Long, List<Integer>> expMerged = expMergeTopProcessingPartitions(expTop0, expTop1, 5);
            NavigableMap<Long, List<Integer>> actMerged = mergeTopProcessingPartitions(actTop0, actTop1, 5);

            System.out.println("act0=" + actTop0);
            System.out.println("act1="+ actTop1);
            System.out.println("res=" + actMerged);

            try {
                assertEquals(expMerged, actMerged);
            } catch (AssertionError e) {
                throw e;
            }
        }
    }

    /**
     * Generation of partitions with their processing time.
     *
     * @param partCnt Number of partitions.
     * @return Mapping: partition id -> processing time in millis.
     */
    private Map<Integer, Long> generateProcessedPartitions(int partCnt) {
        return IntStream.range(0, partCnt).boxed()
            .collect(toMap(identity(), p -> 10L + ThreadLocalRandom.current().nextLong(10)));
    }

    /**
     * Getting expected top (ascending) of the partitions that took the longest processing time.
     *
     * @param processed Mapping: partition id -> processing time in millis.
     * @param max Maximum total number of partitions.
     */
    private NavigableMap<Long, List<Integer>> expTopProcessingPartitions(Map<Integer, Long> processed, int max) {
        TreeMap<Long, List<Integer>> res = processed.entrySet().stream()
            .collect(groupingBy(Map.Entry::getValue, TreeMap::new, mapping(Map.Entry::getKey, toList())));

        a(res, max);

        return res;
    }

    /**
     * Check the maps for equality.
     *
     * @param exp Expected.
     * @param act Actual.
     */
    private void assertEquals(NavigableMap<Long, List<Integer>> exp, NavigableMap<Long, List<Integer>> act) {
        assertEquals(exp.size(), act.size());

        Iterator<Map.Entry<Long, List<Integer>>> iter0 = exp.entrySet().iterator();
        Iterator<Map.Entry<Long, List<Integer>>> iter1 = act.entrySet().iterator();

        while (iter0.hasNext()) {
            Map.Entry<Long, List<Integer>> e0 = iter0.next();
            Map.Entry<Long, List<Integer>> e1 = iter1.next();

            assertEquals(e0.getKey(), e1.getKey());
            assertEqualsCollections(e0.getValue(), e1.getValue());
        }
    }

    private NavigableMap<Long, List<Integer>> expMergeTopProcessingPartitions(
        NavigableMap<Long, List<Integer>> m0,
        NavigableMap<Long, List<Integer>> m1,
        int max
    ) {
        TreeMap<Long, List<Integer>> res = new TreeMap<>(
            m0.entrySet().stream().collect(toMap(Map.Entry::getKey, e -> new ArrayList<>(e.getValue()))));

        System.out.println("m0=" + m0);
        System.out.println("m1=" + m1);

        for (Map.Entry<Long, List<Integer>> e1 : m1.entrySet()) {
            res.merge(e1.getKey(), e1.getValue(), (p0, p1) -> {
                p0.addAll(p1);

                return p0;
            });
        }

        a(res, max);

        System.out.println("res=" + res);

        return res;
    }

    private void a(NavigableMap<Long, List<Integer>> m, int max) {
        int size = 0;

        Set<Long> toRmv = new HashSet<>();

        for (Map.Entry<Long, List<Integer>> e : m.descendingMap().entrySet()) {
            if (e.getValue().size() > max - size)
                e.getValue().subList(0, e.getValue().size() - (max - size)).clear();

            if (e.getValue().isEmpty())
                toRmv.add(e.getKey());
            else
                size += e.getValue().size();
        }

        toRmv.forEach(m::remove);
    }
}
