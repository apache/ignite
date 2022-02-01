/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.db;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.events.CacheRebalancingEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.lang.GridPlainRunnable;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_UNLOADED;
import static org.apache.ignite.events.EventType.EVT_PAGE_REPLACEMENT_STARTED;

/**
 *
 */
public class IgnitePdsPageReplacementDuringPartitionClearTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "cache";

    /** Number of partitions in the test. */
    private static final int PARTS = 128;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAffinity(new RendezvousAffinityFunction(false, PARTS))
            .setRebalanceMode(CacheRebalanceMode.SYNC)
            .setBackups(1);

        cfg.setCacheConfiguration(ccfg);

        // Intentionally set small page cache size.
        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(50L * 1024 * 1024).setPersistenceEnabled(true))
            .setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(memCfg);

        cfg.setIncludeEventTypes(EVT_CACHE_REBALANCE_PART_UNLOADED, EVT_PAGE_REPLACEMENT_STARTED);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 20 * 60 * 1000;
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    @WithSystemProperty(key = GridCacheDatabaseSharedManager.IGNITE_PDS_CHECKPOINT_TEST_SKIP_SYNC, value = "true")
    public void testPageEvictionOnNodeStart() throws Exception {
        cleanPersistenceDir();

        startGrids(2);

        AtomicBoolean stop = new AtomicBoolean(false);

        try {
            Ignite ig = ignite(0);

            ig.cluster().active(true);

            ig.cluster().baselineAutoAdjustEnabled(false);

            int last = loadDataUntilPageReplacement(ignite(0), ignite(1));

            IgniteInternalFuture<?> fut = loadAsync(ig, stop, last);

            EvictionListener evictLsnr = new EvictionListener();

            ignite(0).events().localListen(evictLsnr, EVT_CACHE_REBALANCE_PART_UNLOADED);
            ignite(1).events().localListen(evictLsnr, EVT_CACHE_REBALANCE_PART_UNLOADED);

            IgniteEx igNew = startGrid(2);

            info(">>>>>>>>>>>");
            info(">>>>>>>>>>>");
            info(">>>>>>>>>>>");

            igNew.cluster().setBaselineTopology(3);

            awaitPartitionMapExchange();

            Map<ClusterNode, GridLongList> affinityAfter = allPartitions(igNew);

            evictLsnr.waitPartitionsEvicted(igNew.cluster().localNode(), affinityAfter);

            stop.set(true);

            fut.get();
        }
        finally {
            stop.set(true);

            stopAllGrids();

            cleanPersistenceDir();
        }
    }

    /**
     * @param ig1 Ignite to load.
     * @param ig2 Ignite to load.
     * @return Start index for next updates.
     * @throws IgniteCheckedException If failed.
     */
    private int loadDataUntilPageReplacement(Ignite ig1, Ignite ig2) throws IgniteCheckedException {
        AtomicInteger idx = new AtomicInteger();

        CoundDownFilter lsnr = new CoundDownFilter(EVT_PAGE_REPLACEMENT_STARTED, 2);

        ig1.events().localListen(lsnr, EVT_PAGE_REPLACEMENT_STARTED);
        ig2.events().localListen(lsnr, EVT_PAGE_REPLACEMENT_STARTED);

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new GridPlainRunnable() {
            @Override public void run() {
                IgniteCache<Object, Object> cache = ig1.cache(CACHE_NAME);

                while (!lsnr.isReady()) {
                    int start = idx.getAndAdd(100);

                    Map<Integer, TestValue> putMap = new HashMap<>(100, 1.f);

                    for (int i = 0; i < 100; i++)
                        putMap.put(start + i, new TestValue(start + i));

                    cache.putAll(putMap);
                }
            }
        }, Runtime.getRuntime().availableProcessors(), "initial-load-runner");

        fut.get();

        return idx.get();
    }

    /**
     * Calculates mapping from nodes to partitions.
     *
     * @param ig Ignite instance.
     * @return Map.
     */
    private Map<ClusterNode, GridLongList> allPartitions(Ignite ig) {
        Map<ClusterNode, GridLongList> res = new HashMap<>(ig.cluster().nodes().size(), 1.f);

        Affinity<Object> aff = ig.affinity(CACHE_NAME);

        for (int i = 0; i < PARTS; i++) {
            Collection<ClusterNode> nodes = aff.mapPartitionToPrimaryAndBackups(i);

            for (ClusterNode node : nodes)
                res.computeIfAbsent(node, k -> new GridLongList(2)).add(i);
        }

        return res;
    }

    /**
     * @param ig Ignite instance.
     * @param stopFlag Stop flag.
     * @param start Load key start index.
     * @return Completion future.
     */
    private IgniteInternalFuture<?> loadAsync(Ignite ig, AtomicBoolean stopFlag, int start) {
        AtomicInteger generator = new AtomicInteger(start);

        return GridTestUtils.runMultiThreadedAsync(new GridPlainRunnable() {
            @Override public void run() {
                IgniteCache<Integer, TestValue> cache = ig.cache(CACHE_NAME);

                while (!stopFlag.get()) {
                    int idx = generator.getAndAdd(100);

                    Map<Integer, TestValue> putMap = new HashMap<>(100, 1.f);

                    for (int i = 0; i < 100; i++)
                        putMap.put(idx + i, new TestValue(idx + i));

                    cache.putAll(putMap);
                }
            }
        }, Runtime.getRuntime().availableProcessors(), "load-runner");
    }

    /**
     *
     */
    private static class CoundDownFilter implements IgnitePredicate<Event> {
        /** */
        private final int evtType;

        /** */
        private final AtomicInteger cnt;

        /**
         * @param evtType Event type.
         */
        private CoundDownFilter(int evtType, int cnt) {
            this.evtType = evtType;

            this.cnt = new AtomicInteger(cnt);
        }

        /** {@inheritDoc} */
        @Override public boolean apply(Event evt) {
            if (evt.type() == evtType)
                cnt.decrementAndGet();

            return cnt.get() > 0;
        }

        /**
         * @return Await result.
         */
        public boolean isReady() {
            return cnt.get() <= 0;
        }
    }

    /**
     *
     */
    private static class EvictionListener implements IgnitePredicate<Event> {
        /** */
        private final GridLongList unloadedParts = new GridLongList();

        /** {@inheritDoc} */
        @Override public boolean apply(Event evt) {
            if (evt.type() == EVT_CACHE_REBALANCE_PART_UNLOADED) {
                CacheRebalancingEvent rebEvt = (CacheRebalancingEvent)evt;

                synchronized (this) {
                    unloadedParts.add(rebEvt.partition());

                    unloadedParts.sort();

                    notifyAll();
                }
            }

            return true;
        }

        /**
         * @param node Node to wait.
         * @param affinityAfter Affinity after rebalancing.
         * @throws InterruptedException If calling thread is interrupted.
         */
        public void waitPartitionsEvicted(
            ClusterNode node,
            Map<ClusterNode, GridLongList> affinityAfter
        ) throws InterruptedException {
            GridLongList movedParts = affinityAfter.get(node);

            synchronized (this) {
                while (!unloadedParts.equals(movedParts))
                    wait();
            }
        }
    }

    /**
     *
     */
    private static class TestValue {
        /** */
        private int id;

        /** */
        private final byte[] payload = new byte[512];

        /**
         * @param id ID.
         */
        private TestValue(int id) {
            this.id = id;
        }

        /**
         * @return ID.
         */
        public int getId() {
            return id;
        }

        /**
         * @return Payload.
         */
        public boolean hasPayload() {
            return payload != null;
        }
    }
}
