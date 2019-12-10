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

package org.apache.ignite.internal.processors.cache.persistence;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.verify.ValidateIndexesClosure;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesJobResult;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BASELINE_AUTO_ADJUST_ENABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_FILE_REBALANCE_ENABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_FILE_REBALANCE_THRESHOLD;

/**
 * todo should be removed, CacheFileRebalancingSelfTest should be able to check indexes (it must be included into indexes suite)
 */
public class IndexedCacheFileRebalancingTest extends GridCommonAbstractTest {
    /** Cache with enabled indexes. */
    private static final String INDEXED_CACHE = "indexed";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        cfg.setRebalanceThreadPoolSize(2);

//        CacheConfiguration ccfg1 = cacheConfiguration(CACHE)
//            .setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_SAFE)
//            .setBackups(2)
//            .setRebalanceMode(CacheRebalanceMode.ASYNC)
//            .setIndexedTypes(Integer.class, Integer.class)
//            .setAffinity(new RendezvousAffinityFunction(false, 32))
//            .setRebalanceBatchesPrefetchCount(2)
//            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        CacheConfiguration ccfg2 = cacheConfiguration(INDEXED_CACHE)
            .setBackups(0)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        QueryEntity qryEntity = new QueryEntity(Integer.class.getName(), TestValue.class.getName());

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("v1", Integer.class.getName());
        fields.put("v2", Integer.class.getName());

        qryEntity.setFields(fields);

        QueryIndex qryIdx = new QueryIndex("v1", true);

        qryEntity.setIndexes(Collections.singleton(qryIdx));

        ccfg2.setQueryEntities(Collections.singleton(qryEntity));

        CacheConfiguration[] cacheCfgs = new CacheConfiguration[1];
        cacheCfgs[0] = ccfg2;

//        if (filteredCacheEnabled && !gridName.endsWith("0")) {
//            CacheConfiguration ccfg3 = cacheConfiguration(FILTERED_CACHE)
//                .setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_SAFE)
//                .setBackups(2)
//                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
//                .setNodeFilter(new IgnitePdsCacheRebalancingAbstractTest.CoordinatorNodeFilter());
//
//            cacheCfgs.add(ccfg3);
//        }

        cfg.setCacheConfiguration(cacheCfgs);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setConcurrencyLevel(Runtime.getRuntime().availableProcessors() * 4)
            .setCheckpointFrequency(3_000)
            .setWalMode(WALMode.LOG_ONLY)
            .setPageSize(1024)
            .setWalSegmentSize(8 * 1024 * 1024) // For faster node restarts with enabled persistence.
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setName("dfltDataRegion")
                .setPersistenceEnabled(true)
                .setMaxSize(512 * 1024 * 1024)
            );

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    protected CacheConfiguration cacheConfiguration(String cacheName) {
        CacheConfiguration ccfg = new CacheConfiguration(cacheName);

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        ccfg.setBackups(1);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    @Test
    @WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    @WithSystemProperty(key = IGNITE_PDS_FILE_REBALANCE_THRESHOLD, value="0")
    public void testMultipleCachesCancelRebalanceConstantLoadPartitioned() throws Exception {
        List<ClusterNode> blt = new ArrayList<>();

        int entriesCnt = 100_000;

        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        blt.add(ignite0.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        int threads = Runtime.getRuntime().availableProcessors() / 2;

        loadData(ignite0, INDEXED_CACHE, entriesCnt);

        AtomicInteger cntr = new AtomicInteger(entriesCnt);

        ConstantLoader ldr = new ConstantLoader(ignite0.cache(INDEXED_CACHE), cntr, false, threads);

        IgniteInternalFuture ldrFut = GridTestUtils.runMultiThreadedAsync(ldr, threads, "thread");

        forceCheckpoint(ignite0);

        IgniteEx ignite1 = startGrid(1);

        blt.add(ignite1.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        U.sleep(ThreadLocalRandom.current().nextLong(80));

        IgniteEx ignite2 = startGrid(2);

        blt.add(ignite2.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        U.sleep(ThreadLocalRandom.current().nextLong(80));

        IgniteEx ignite3 = startGrid(3);

        blt.add(ignite3.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        awaitPartitionMapExchange();

        ldr.stop();

        ldrFut.get();

        verifyCacheContent(ignite2.cache(INDEXED_CACHE), cntr.get());

        // Validate indexes on start.
        ValidateIndexesClosure clo = new ValidateIndexesClosure(Collections.singleton(INDEXED_CACHE), 0, 0);

        ignite0.cluster().active(false);

        ignite1.context().resource().injectGeneric(clo);

        VisorValidateIndexesJobResult res = clo.call();

        assertFalse(res.hasIssues());

        ignite2.context().resource().injectGeneric(clo);

        res = clo.call();

        assertFalse(res.hasIssues());
    }

    @Test
    @WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    @WithSystemProperty(key = IGNITE_PDS_FILE_REBALANCE_THRESHOLD, value="0")
    public void testIndexedCacheStartStopLastNodeConstantLoadPartitioned() throws Exception {
        List<ClusterNode> blt = new ArrayList<>();

        int entriesCnt = 100_000;

        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        blt.add(ignite0.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        int threads = Runtime.getRuntime().availableProcessors() / 2;

        loadData(ignite0, INDEXED_CACHE, entriesCnt);

        AtomicInteger cntr = new AtomicInteger(entriesCnt);

        ConstantLoader ldr = new ConstantLoader(ignite0.cache(INDEXED_CACHE), cntr, false, threads);

        IgniteInternalFuture ldrFut = GridTestUtils.runMultiThreadedAsync(ldr, threads, "thread");

        forceCheckpoint(ignite0);

        IgniteEx ignite1 = startGrid(1);

        blt.add(ignite1.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        U.sleep(ThreadLocalRandom.current().nextLong(80));

        IgniteEx ignite2 = startGrid(2);

        blt.add(ignite2.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        U.sleep(ThreadLocalRandom.current().nextLong(80));

        IgniteEx ignite3 = startGrid(3);

        ClusterNode node3 = ignite3.localNode();

        blt.add(ignite3.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        U.sleep(ThreadLocalRandom.current().nextLong(50));

        stopGrid(3);

        blt.remove(node3);

        ignite0.cluster().setBaselineTopology(blt);

        U.sleep(ThreadLocalRandom.current().nextLong(100));

        ignite3 = startGrid(3);

        blt.add(ignite3.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        awaitPartitionMapExchange();

        ldr.stop();

        ldrFut.get();

        verifyCacheContent(ignite2.cache(INDEXED_CACHE), cntr.get());

        // Validate indexes on start.
        ValidateIndexesClosure clo = new ValidateIndexesClosure(Collections.singleton(INDEXED_CACHE), 0, 0);

        ignite0.cluster().active(false);

        ignite1.context().resource().injectGeneric(clo);

        VisorValidateIndexesJobResult res = clo.call();

        assertFalse(res.hasIssues());

        ignite2.context().resource().injectGeneric(clo);

        res = clo.call();

        assertFalse(res.hasIssues());
    }

    @Test
    @WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    @WithSystemProperty(key = IGNITE_PDS_FILE_REBALANCE_THRESHOLD, value="0")
    public void checkIndexEvict() throws Exception {
        IgniteEx node0 = startGrid(0);

        node0.cluster().active(true);

        IgniteInternalCache cache = node0.cachex(INDEXED_CACHE);

        CacheGroupContext grp = cache.context().group();

        GridCacheSharedContext<Object, Object> cctx = node0.context().cache().context();

        node0.context().cache().context().database().checkpointReadLock();

        try {
            int tag = ((PageMemoryEx)grp.dataRegion().pageMemory()).invalidate(grp.groupId(), PageIdAllocator.INDEX_PARTITION);

            ((FilePageStoreManager)cctx.pageStore()).getStore(grp.groupId(), PageIdAllocator.INDEX_PARTITION).truncate(tag);
        } finally {
            node0.context().cache().context().database().checkpointReadUnlock();
        }

        assert !cctx.pageStore().exists(grp.groupId(), PageIdAllocator.INDEX_PARTITION);

        cache.context().offheap().start(cctx, grp);

        assert cctx.pageStore().exists(grp.groupId(), PageIdAllocator.INDEX_PARTITION);

        //qryProc.rebuildIndexesFromHash(ctx)

        final ConcurrentMap<Integer, TestValue> map = new ConcurrentHashMap<>();

        try (IgniteDataStreamer<Integer, TestValue> ds = node0.dataStreamer(INDEXED_CACHE)) {
            for (int i = 0; i < 10_000; i++) {
                ds.addData(i, new TestValue(i, i, i));
                map.put(i, new TestValue(i, i, i));
            }
        }

//        forceCheckpoint();
//
//        startGrid(1);
//
//        node0.cluster().setBaselineTopology(2);
//
//        awaitPartitionMapExchange();
//
//        for (int i = 10_000; i < 11_000; i++)
//            node0.cache(INDEXED_CACHE).put(i, new TestValue(i, i, i));
    }

    @Test
    @WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    @WithSystemProperty(key = IGNITE_PDS_FILE_REBALANCE_THRESHOLD, value="0")
    public void checkIndexEvictRebuild() throws Exception {
        IgniteEx node0 = startGrid(0);

        node0.cluster().active(true);

        IgniteInternalCache cache = node0.cachex(INDEXED_CACHE);

        CacheGroupContext grp = cache.context().group();

        GridCacheSharedContext<Object, Object> cctx = node0.context().cache().context();

        try (IgniteDataStreamer<Integer, TestValue> ds = node0.dataStreamer(INDEXED_CACHE)) {
            for (int i = 0; i < 10_000; i++)
                ds.addData(i, new TestValue(i, i, i));
        }

        U.sleep(1_000);

        node0.context().cache().context().database().checkpointReadLock();

        try {
            int tag = ((PageMemoryEx)grp.dataRegion().pageMemory()).invalidate(grp.groupId(), PageIdAllocator.INDEX_PARTITION);

            ((FilePageStoreManager)cctx.pageStore()).getStore(grp.groupId(), PageIdAllocator.INDEX_PARTITION).truncate(tag);
        } finally {
            node0.context().cache().context().database().checkpointReadUnlock();
        }

        assert !cctx.pageStore().exists(grp.groupId(), PageIdAllocator.INDEX_PARTITION);

        log.info(">>>>> start");

        GridQueryProcessor qryProc = cctx.kernalContext().query();

        GridCacheContextInfo cacheInfo = new GridCacheContextInfo(cache.context(), false);

        cache.context().offheap().start(cctx, grp);

        qryProc.onCacheStop0(cacheInfo, false);
        qryProc.onCacheStart0(cacheInfo, node0.context().cache().cacheDescriptor(INDEXED_CACHE).schema(), node0.context().cache().cacheDescriptor(INDEXED_CACHE).sql());
//
//
//        cctx.kernalContext().query().onCacheStart(new GridCacheContextInfo(cache.context(), false),
//            );

        assert cctx.pageStore().exists(grp.groupId(), PageIdAllocator.INDEX_PARTITION);

        log.info(">>>>> started");

        assert qryProc.moduleEnabled();

        qryProc.rebuildIndexesFromHash(cache.context()).get();

        cache.put(100_000, new TestValue(100_000, 100_000, 100_000));

//        forceCheckpoint();
//
//        startGrid(1);
//
//        node0.cluster().setBaselineTopology(2);
//
//        awaitPartitionMapExchange();
//
//        for (int i = 10_000; i < 11_000; i++)
//            node0.cache(INDEXED_CACHE).put(i, new TestValue(i, i, i));
    }


    /** {@inheritDoc} */
    @Override protected long getPartitionMapExchangeTimeout() {
        return 45_000;
    }

    /**
     *
     */
    private static class TestValue implements Serializable {
        /** Operation order. */
        private final long order;

        /** V 1. */
        private final int v1;

        /** V 2. */
        private final int v2;

        /** Flag indicates that value has removed. */
        private final boolean removed;

        private TestValue(long order, int v1, int v2) {
            this(order, v1, v2, false);
        }

        private TestValue(long order, int v1, int v2, boolean removed) {
            this.order = order;
            this.v1 = v1;
            this.v2 = v2;
            this.removed = removed;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            TestValue testValue = (TestValue) o;

            return order == testValue.order &&
                v1 == testValue.v1 &&
                v2 == testValue.v2;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(order, v1, v2);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestValue{" +
                "order=" + order +
                ", v1=" + v1 +
                ", v2=" + v2 +
                '}';
        }
    }

    private void verifyCacheContent(IgniteCache<Object, Object> cache, long cnt) {
        verifyCacheContent(cache, cnt, false);
    }

    // todo should check partitions
    private void verifyCacheContent(IgniteCache<Object, Object> cache, long cnt, boolean removes) {
        log.info("Verifying cache contents [cache=" + cache.getName() + ", size=" + cnt + "]");

        StringBuilder buf = new StringBuilder();

        int fails = 0;

        for (int k = 0; k < cnt; k++) {
            if (removes && k % 10 == 0)
                continue;

            TestValue exp = new TestValue(k, k, k);;
            TestValue actual = (TestValue)cache.get(k);

            if (!Objects.equals(exp, actual)) {
                if (fails++ < 100)
                    buf.append("cache=").append(cache.getName()).append(", key=").append(k).append(", expect=").append(exp).append(", actual=").append(actual).append('\n');
                else {
                    buf.append("\n... and so on\n");

                    break;
                }
            }

            if ((k + 1) % (cnt / 10) == 0)
                log.info("Verification: " + (k + 1) * 100 / cnt + "%");
        }

        if (!removes && cnt != cache.size())
            buf.append("\ncache=").append(cache.getName()).append(" size mismatch [expect=").append(cnt).append(", actual=").append(cache.size()).append('\n');

        assertTrue(buf.toString(), buf.length() == 0);
    }

    /**
     * @param ignite Ignite instance to load.
     * @param name The cache name to add random data to.
     * @param size The total size of entries.
     */
    private void loadData(Ignite ignite, String name, int size) {
        try (IgniteDataStreamer<Integer, TestValue> streamer = ignite.dataStreamer(name)) {
            streamer.allowOverwrite(true);

            for (int i = 0; i < size; i++) {
                if ((i + 1) % (size / 10) == 0)
                    log.info("Prepared " + (i + 1) * 100 / (size) + "% entries.");

                streamer.addData(i, new TestValue(i, i, i));
            }
        }
    }

    /** */
    private static class ConstantLoader implements Runnable {
        /** */
        private final AtomicInteger cntr;

        /** */
        private final boolean enableRemove;

        /** */
        private final CyclicBarrier pauseBarrier;

        /** */
        private volatile boolean pause;

        /** */
        private volatile boolean paused;

        /** */
        private volatile boolean stop;

        /** */
        private final IgniteCache<Integer, TestValue> cache;

        /** */
        public ConstantLoader(IgniteCache<Integer, TestValue> cache, AtomicInteger cntr, boolean enableRemove, int threadCnt) {
            this.cache = cache;
            this.cntr = cntr;
            this.enableRemove = enableRemove;
            this.pauseBarrier = new CyclicBarrier(threadCnt + 1); // +1 waiter
        }

        /** {@inheritDoc} */
        @Override public void run() {
            String cacheName = cache.getName();

            while (!stop && !Thread.currentThread().isInterrupted()) {
                if (pause) {
                    if (!paused) {
                        U.awaitQuiet(pauseBarrier);

                        log.info("Async loader paused.");

                        paused = true;
                    }

                    // Busy wait for resume.
                    try {
                        U.sleep(100);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        break;
                    }

                    continue;
                }

                int from = cntr.getAndAdd(100);

                for (int i = from; i < from + 100; i++)
                    cache.put(i, new TestValue(i, i, i));

                if (!enableRemove)
                    continue;

                for (int i = from; i < from + 100; i += 10)
                    cache.remove(i);
            }

            log.info("Async loader stopped.");
        }

        /**
         * Stop loader thread.
         */
        public void stop() {
            stop = true;
        }

        /**
         * Pause loading.
         */
        public void pause() {
            pause = true;

            log.info("Suspending loader threads: " + pauseBarrier.getParties());

            // Wait all workers came to barrier.
            U.awaitQuiet(pauseBarrier);

            log.info("Loader suspended");
        }

        /**
         * Resume loading.
         */
        public void resume() {
            paused = false;
            pause = false;
        }
    }
}
