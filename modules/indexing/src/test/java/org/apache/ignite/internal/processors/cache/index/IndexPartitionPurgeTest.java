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

package org.apache.ignite.internal.processors.cache.index;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointEntry;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointEntryType;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.future.CountDownFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests for index partition purge.
 */
public class IndexPartitionPurgeTest extends GridCommonAbstractTest {
    /** */
    private static final int NUM_KEYS = 1_000;

    /** */
    private static final int NUM_PARTS = 10;

    /** */
    private static final long TIMEOUT = 600_000L;

    /** */
    private int maxInlineSize = 20;

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TIMEOUT;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration igniteCfg = super.getConfiguration(igniteInstanceName);

        igniteCfg.setAutoActivationEnabled(false);
        igniteCfg.setConsistentId(igniteInstanceName);

        igniteCfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setWalMode(WALMode.LOG_ONLY)
                .setWalSegmentSize(16 * 1024 * 1024)
                .setCheckpointFrequency(20 * 60 * 1000)
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setMaxSize(1024 * 1024 * 1024)
                )
            );

        CacheConfiguration ccfg1 = getCacheConfiguration("cache1")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        CacheConfiguration ccfg2 = getCacheConfiguration("cache2")
                .setAtomicityMode(CacheAtomicityMode.ATOMIC);

        ccfg1.setGroupName("group1");
        ccfg2.setGroupName("group1");

        CacheConfiguration ccfg3 = getCacheConfiguration("cache3");

        igniteCfg.setCacheConfiguration(ccfg1, ccfg2, ccfg3);

        return igniteCfg;
    }

    /**
     *
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration getCacheConfiguration(String cacheName) {
        return new CacheConfiguration<Integer, TestValueA>(cacheName)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(NUM_PARTS))
            .setBackups(1)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setCacheMode(CacheMode.PARTITIONED)
            .setQueryEntities(F.asList(
                    getQueryEntity(TestValueA.class),
                    getQueryEntity(TestValueB.class)))
            .setSqlIndexMaxInlineSize(maxInlineSize)
            .setOnheapCacheEnabled(true)
            .setSqlOnheapCacheEnabled(true);
    }

    /**
     *
     * @param cls class.
     * @return Query Entity.
     */
    private QueryEntity getQueryEntity(Class<? extends TestValueA> cls) {
        String name = cls.getSimpleName();

        QueryIndex idxAB = new QueryIndex()
                .setName("IDX_" + name + "_AB")
                .setIndexType(QueryIndexType.SORTED)
                .setFieldNames(F.asList("A", "B"), true);

        QueryIndex idxC = new QueryIndex()
                .setName("IDX_" + name + "_C")
                .setIndexType(QueryIndexType.SORTED)
                .setFieldNames(F.asList("C"), true);

        return new QueryEntity()
                .setTableName("TBL_"+ name)
                .setKeyType(Integer.class.getName())
                .setValueType(cls.getName())
                .addQueryField("f1", String.class.getName(), "A")
                .addQueryField("f2", Integer.class.getName(), "B")
                .addQueryField("f3", String.class.getName(), "C")
                .setIndexes(F.asList(idxAB, idxC));
    }

    /**
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionPurgeInlineSize0() throws Exception {
        maxInlineSize = 0;

        doTestPartitionPurge("cache1");
    }

    /**
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionPurge() throws Exception {
        doTestPartitionPurge("cache1");
    }

    /**
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionPurgeNoCacheGroup() throws Exception {
        doTestPartitionPurge("cache3");
    }

    /**
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    private void doTestPartitionPurge(String cacheName) throws Exception {
        startGrids(2);

        IgniteEx grid = grid(0);

        grid.cluster().baselineAutoAdjustEnabled(false);

        grid.cluster().active(true);

        awaitPartitionMapExchange();

        fillData(grid);

        IgniteCache cache = grid.cache(cacheName);

        GridCacheContext cctx = grid.cachex(cacheName).context();

        List<Integer> partIds = evictingPartitionsAfterJoin(grid, cache, NUM_PARTS);

        List<GridDhtLocalPartition> parts = partIds.stream().map(id-> cctx.topology().localPartition(id))
            .collect(Collectors.toList());

        parts.forEach(GridDhtLocalPartition::reserve);

        startGrid(2);

        parts.forEach(GridDhtLocalPartition::moving);

        IgniteInternalFuture<Void> fut = cctx.shared().evict().purgePartitionsExclusively(cctx.group(), parts);

        parts.forEach(GridDhtLocalPartition::release);

        grid.cluster().setBaselineTopology(3);

        grid.cluster().active(true);

        fut.get(getTestTimeout());

        if (cctx.group().sharedGroup()) {
            for (GridCacheContext cctx0 : cctx.group().caches())
                checkIndexRowsArePurged(grid, cctx0.name(), partIds);
        }
        else
            checkIndexRowsArePurged(grid, cacheName, partIds);

        grid(2).cachex(cacheName).cache().context().group().preloader().rebalanceFuture().get(getTestTimeout());

        stopGrid(2);

        grid.cluster().setBaselineTopology(4);

        grid.cluster().active(true);

        grid.cachex(cacheName).cache().context().group().preloader().rebalanceFuture().get(getTestTimeout());

        assertPartitionsSame(idleVerify(grid, cacheName));
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"ConstantConditions", "ThrowableNotThrown"})
    @Test
    public void testCacheStopDuringPartitionPurge() throws Exception {
        GridQueryProcessor.idxCls = BlockPurgeIndexing.class;

        startGrid(0);

        GridQueryProcessor.idxCls = null;

        startGrid(1);

        IgniteEx grid = grid(0);

        grid.cluster().baselineAutoAdjustEnabled(false);

        grid.cluster().active(true);

        awaitPartitionMapExchange();

        fillData(grid);

        IgniteCache cache = grid.cache("cache1");

        GridCacheContext cctx = grid.cachex("cache1").context();

        List<Integer> partIds = evictingPartitionsAfterJoin(grid, cache, NUM_PARTS);

        List<GridDhtLocalPartition> parts = partIds.stream().map(id -> cctx.topology().localPartition(id))
            .collect(Collectors.toList());

        parts.forEach(GridDhtLocalPartition::reserve);

        startGrid(2);

        parts.forEach(GridDhtLocalPartition::moving);

        IgniteInternalFuture<Void> fut = cctx.shared().evict().purgePartitionsExclusively(cctx.group(), parts);

        parts.forEach(GridDhtLocalPartition::release);

        BlockPurgeIndexing blkIdx = (BlockPurgeIndexing)grid.context().query().getIndexing();

        blkIdx.waitPhase1();

        stopGrid(0);

        blkIdx.startPhase2();

        GridTestUtils.assertThrows(log, () -> fut.get(getTestTimeout()),
            IgniteCheckedException.class, "is stopping");
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    @Test
    public void testSinglePartitionClearAfterExclusivePurge() throws Exception {
        startGrids(2);

        IgniteEx grid = grid(0);

        grid.cluster().baselineAutoAdjustEnabled(false);

        grid.cluster().active(true);

        awaitPartitionMapExchange();

        fillData(grid);

        GridCacheContext cctx = grid.cachex("cache1").context();

        List<Integer> partIds = F.asList(0, 4, 8);

        List<GridDhtLocalPartition> parts = partIds.stream().map(id -> cctx.topology().localPartition(id))
            .collect(Collectors.toList());

        parts.forEach(GridDhtLocalPartition::moving);

        IgniteInternalFuture<Void> fut = cctx.shared().evict().purgePartitionsExclusively(cctx.group(), parts);

        fut.get(getTestTimeout());

        parts.forEach(GridDhtLocalPartition::clearAsync);

        CountDownFuture clFut = new CountDownFuture(partIds.size());

        parts.forEach(p->p.onClearFinished(f -> {
            if (f.error() == null)
                clFut.onDone();
            else
                clFut.onDone(f.error());
        }));

        clFut.get(getTestTimeout());

        parts.forEach(p->assertEquals(0, p.fullSize()));
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    @Test
    public void testPartitionPurgeOnTwoCacheGroups() throws Exception {
        startGrids(2);

        IgniteEx grid = grid(0);

        grid.cluster().baselineAutoAdjustEnabled(false);

        grid.cluster().active(true);

        awaitPartitionMapExchange();

        fillData(grid);

        GridCacheContext cctx1 = grid.cachex("cache1").context();
        GridCacheContext cctx2 = grid.cachex("cache3").context();

        List<Integer> partIds = F.asList(0, 4, 8);

        List<GridDhtLocalPartition> parts1 = partIds.stream().map(id -> cctx1.topology().localPartition(id))
            .collect(Collectors.toList());

        List<GridDhtLocalPartition> parts2 = partIds.stream().map(id -> cctx2.topology().localPartition(id))
            .collect(Collectors.toList());

        List<GridDhtLocalPartition> parts = new ArrayList<>(parts1);
        parts.addAll(parts2);

        parts.forEach(GridDhtLocalPartition::moving);

        IgniteInternalFuture<Void> fut1 = cctx1.shared().evict().purgePartitionsExclusively(cctx1.group(), parts1);
        IgniteInternalFuture<Void> fut2 = cctx2.shared().evict().purgePartitionsExclusively(cctx2.group(), parts2);

        fut1.get(getTestTimeout());
        fut2.get(getTestTimeout());

        checkIndexRowsArePurged(grid, "cache1", partIds);
        checkIndexRowsArePurged(grid, "cache2", partIds);
        checkIndexRowsArePurged(grid, "cache3", partIds);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    @Test
    public void testSimultaneousClearAndPurgeTwoCacheGroups() throws Exception {
        startGrids(2);

        IgniteEx grid = grid(0);

        grid.cluster().baselineAutoAdjustEnabled(false);

        grid.cluster().active(true);

        awaitPartitionMapExchange();

        fillData(grid);

        GridCacheContext cctx1 = grid.cachex("cache1").context();
        GridCacheContext cctx2 = grid.cachex("cache3").context();

        List<Integer> partIds = F.asList(0, 4, 8);

        List<GridDhtLocalPartition> parts1 = partIds.stream().map(id -> cctx1.topology().localPartition(id))
            .collect(Collectors.toList());

        List<GridDhtLocalPartition> parts2 = partIds.stream().map(id -> cctx2.topology().localPartition(id))
            .collect(Collectors.toList());

        List<GridDhtLocalPartition> parts = new ArrayList<>(parts1);

        parts.addAll(parts2);

        parts.forEach(GridDhtLocalPartition::moving);

        IgniteInternalFuture<Void> fut1 = cctx1.shared().evict().purgePartitionsExclusively(cctx1.group(), parts1);

        parts2.forEach(GridDhtLocalPartition::clearAsync);

        CountDownFuture clFut = new CountDownFuture(partIds.size());

        parts2.forEach(p->p.onClearFinished(f -> {
            if (f.error() == null)
                clFut.onDone();
            else
                clFut.onDone(f.error());
        }));

        fut1.get(getTestTimeout());
        clFut.get(getTestTimeout());

        checkIndexRowsArePurged(grid, "cache1", partIds);
        checkIndexRowsArePurged(grid, "cache2", partIds);
        checkIndexRowsArePurged(grid, "cache3", partIds);

        parts2.forEach(p->assertEquals(0, p.fullSize()));
    }

    /**
     * Check that wal delta records are applied OK.
     */
    @SuppressWarnings("ConstantConditions")
    @Test
    public void testWalRecovery() throws Exception {
        IgniteEx grid = startGrid(0);

        grid.cluster().baselineAutoAdjustEnabled(false);

        grid.cluster().active(true);

        awaitPartitionMapExchange();

        fillData(grid);

        GridCacheContext cctx = grid.cachex("cache1").context();

        List<Integer> partIds = F.asList(0, 4, 8);

        List<GridDhtLocalPartition> parts = partIds.stream().map(id -> cctx.topology().localPartition(id))
            .collect(Collectors.toList());

        parts.forEach(GridDhtLocalPartition::moving);

        IgniteInternalFuture<Void> fut = cctx.shared().evict().purgePartitionsExclusively(cctx.group(), parts);

        fut.get(getTestTimeout());

        partIds = F.asList(1, 2, 3, 5, 6, 7, 9);

        parts = partIds.stream().map(id -> cctx.topology().localPartition(id))
            .collect(Collectors.toList());

        parts.forEach(GridDhtLocalPartition::moving);

        fut = cctx.shared().evict().purgePartitionsExclusively(cctx.group(), parts);

        fut.get(getTestTimeout());

        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)cctx.shared().database();

        db.forceCheckpoint("test").finishFuture().get();

        db.enableCheckpoints(false).get();

        CheckpointEntry cpEntry = db.checkpointHistory().lastCheckpoint();

        String cpEndFileName = GridCacheDatabaseSharedManager.checkpointFileName(cpEntry, CheckpointEntryType.END);

        Files.delete(Paths.get(db.checkpointDirectory().getAbsolutePath(), cpEndFileName));

        stopGrid(0);

        grid = startGrid(0);

        grid.cluster().active(true);
    }

    /**
     * Check that indexes do not return rows from purged partitions.
     *
     * @param ignite Ignite.
     * @param cacheName Cache name.
     * @param purgedParts Id of partitions that were purged.
     */
    private void checkIndexRowsArePurged(IgniteEx ignite, String cacheName, List<Integer> purgedParts) {
        long cnt = 0L;

        for (int k = 0; k < NUM_KEYS/2; k++) {
            if (!purgedParts.contains(k % NUM_PARTS)) // ~ cctx.affinity().partition(k)
                cnt++;
        }

        checkIndexRowsArePurged(ignite, "\"" + cacheName + "\".TBL_TESTVALUEA", purgedParts, cnt);
        checkIndexRowsArePurged(ignite, "\"" + cacheName + "\".TBL_TESTVALUEB", purgedParts, cnt);
    }

    /**
     * Check that indexes do not return rows from purged partitions.
     *
     * @param ignite Ignite.
     * @param schemaTable Schema and table name.
     * @param purgedParts Id of partitions that were purged.
     * @param cnt Count of rows expected to exist overall (for all other partitions).
     */
    private void checkIndexRowsArePurged(IgniteEx ignite, String schemaTable, List<Integer> purgedParts, long cnt) {
        List<List<?>> r = ignite.context().query().querySqlFields(
            new SqlFieldsQuery("SELECT COUNT(1) FROM " + schemaTable + " WHERE A > ? AND B > ?")
                .setLocal(true)
                .setArgs(Integer.toString(0), 0)
                .setPartitions(U.toIntArray(purgedParts))
            , false).getAll();

        assertEquals(0L, r.get(0).get(0));

        r = ignite.context().query().querySqlFields(
            new SqlFieldsQuery("SELECT COUNT(1) FROM " + schemaTable + " WHERE C > ?")
                .setLocal(true)
                .setArgs(Integer.toString(0))
                .setPartitions(U.toIntArray(purgedParts))
            , false).getAll();

        assertEquals(0L, r.get(0).get(0));

        r = ignite.context().query().querySqlFields(
            new SqlFieldsQuery("SELECT COUNT(1) FROM " + schemaTable)
                .setLocal(true)
                .setPartitions(IntStream.range(0, NUM_PARTS).toArray())
            , false).getAll();

        assertEquals(cnt, r.get(0).get(0));
    }

    /**
     *
     * @param ignite Ignite.
     * @throws Exception If failed.
     */
    private void fillData(IgniteEx ignite) throws Exception {
        Collection<String> caches = F.asList("cache1", "cache2", "cache3");

        ignite.context().cache().changeWalMode(caches, false).get();

        fillData(ignite, "cache1");
        fillData(ignite, "cache2");
        fillData(ignite, "cache3");

        forceCheckpoint(ignite);

        ignite.context().cache().changeWalMode(caches, true).get();
    }

    /**
     *
     * @param ignite Ignite.
     * @param cacheName Cache name.
     */
    private void fillData(IgniteEx ignite, String cacheName) {
        IgniteCache<Integer, TestValueA> cache = ignite.cache(cacheName);

        for (int idx = 0; idx < NUM_KEYS/2; idx ++) {
            cache.put(idx, new TestValueA(Integer.toString(idx), idx, Integer.toString(idx)));
            cache.put(NUM_KEYS - idx, new TestValueB(Integer.toString(idx), idx, Integer.toString(idx)));
        }
    }

    /**
     * Value type for the first table.
     */
    public static class TestValueA {
        /** */
        private String f1;

        /** */
        private int f2;

        /** */
        private String f3;

        /**
         * @param f1 Field 1 value.
         * @param f2 Field 2 value.
         * @param f3 Field 3 value.
         */
        public TestValueA(String f1, int f2, String f3) {
            this.f1 = f1;
            this.f2 = f2;
            this.f3 = f3;
        }

        /**
         * @return Field 1 value.
         */
        public String f1() {
            return f1;
        }

        /**
         * @return Field 2 value.
         */
        public int f2() {
            return f2;
        }

        /**
         * @return Field 3 value.
         */
        public String f3() {
            return f3;
        }
    }

    /**
     * Value type for the second table.
     */
    public static class TestValueB extends TestValueA {
        /**
         * @param f1 Field 1 value.
         * @param f2 Field 2 value.
         * @param f3 Field 3 value.
         */
        TestValueB(String f1, int f2, String f3) {
            super(f1, f2, f3);
        }
    }

    /**
     * Blocks the last index purge task before it starts.
     */
    private static class BlockPurgeIndexing extends IgniteH2Indexing {
        /** */
        private AtomicInteger cnt = new AtomicInteger();

        /** */
        CountDownLatch phase1 = new CountDownLatch(1);

        /** */
        volatile CountDownLatch phase2;


        /** {@inheritDoc} */
        @Override public List<IgniteBiTuple<Runnable, IgniteInternalFuture<Void>>> purgeIndexPartitions(
            CacheGroupContext grp, Set<Integer> parts) {
            List<IgniteBiTuple<Runnable, IgniteInternalFuture<Void>>> res = super.purgeIndexPartitions(grp, parts);

            cnt.set(res.size());

            phase2 = new CountDownLatch(res.size());

            return res.stream().map(this::wrap).collect(Collectors.toList());
        }

        /**
         * Wait for latch 1.
         */
        public void waitPhase1() {
            try {
                phase1.await(TIMEOUT, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        /**
         * Signal latch 2.
         */
        public void startPhase2() {
            phase2.countDown();
        }

        /**
         * Wrap a tuple and embed synchronization into runnable.
         *
         * @param t Tuple.
         * @return Wrapped tuple.
         */
        private IgniteBiTuple<Runnable, IgniteInternalFuture<Void>> wrap(
            IgniteBiTuple<Runnable, IgniteInternalFuture<Void>> t) {

            return new IgniteBiTuple<>(() -> {
                if (cnt.decrementAndGet() == 0) {
                    phase1.countDown();

                    try {
                        phase2.await(TIMEOUT, TimeUnit.MILLISECONDS);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }

                try {
                    t.get1().run();
                }
                finally {
                    phase2.countDown();
                }
            }, t.get2());
        }
    }
}
