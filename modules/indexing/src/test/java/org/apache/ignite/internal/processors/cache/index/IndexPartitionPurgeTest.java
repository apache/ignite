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

import java.util.Arrays;
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
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 */
@RunWith(Parameterized.class)
public class IndexPartitionPurgeTest extends GridCommonAbstractTest {
    /** */
    private final static int NUM_KEYS = 1_000;

    /** */
    private final static int NUM_PARTS = 10;

    /** */
    private final static long TIMEOUT = 600_000L;

    /** */
    @Parameterized.Parameter(0)
    public int maxInlineSize;

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

    @Parameterized.Parameters(name = "maxInlineSize={0}")
    public static Collection parameters() {
        return Arrays.asList(new Object[][] {
            {0},
            //{20},
        });
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration igniteCfg = super.getConfiguration(igniteInstanceName);

        igniteCfg.setAutoActivationEnabled(false);
        igniteCfg.setConsistentId(igniteInstanceName);

        igniteCfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(true).setMaxSize(1024 * 1024 * 1024)
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
            .setCacheMode(CacheMode.PARTITIONED) // REPLICATED cache partitions are never evicted, but may be cleared alright.
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
    @SuppressWarnings("ConstantConditions")
    @Test
    public void testPartitionSetEvict() throws Exception {
        startGrids(2);

        IgniteEx grid = grid(0);

        grid.cluster().baselineAutoAdjustEnabled(false);

        grid.cluster().active(true);

        awaitPartitionMapExchange();

        fillData(grid);

        IgniteCache cache = grid.cache("cache1");

        GridCacheContext cctx = grid.cachex("cache1").context();

        List<Integer> partIds = evictingPartitionsAfterJoin(grid, cache, NUM_PARTS);

        List<GridDhtLocalPartition> parts = partIds.stream().map(id-> cctx.topology().localPartition(id))
            .collect(Collectors.toList());

        parts.forEach(GridDhtLocalPartition::reserve);

        startGrid(2);

        parts.forEach(GridDhtLocalPartition::moving);

        IgniteInternalFuture<Void> fut = cctx.shared().evict().evictPartitionsExclusively(cctx.group(), parts);

        parts.forEach(GridDhtLocalPartition::release);

        grid.cluster().setBaselineTopology(3);

        grid.cluster().active(true);

        fut.get(getTestTimeout());

        for (GridDhtLocalPartition part : parts) {
            assertTrue(part.markForDestroy());

            part.setState(GridDhtPartitionState.EVICTED);

            part.destroy();

            part.awaitDestroy();
        }

        checkIndexRows(grid, partIds);

        grid(2).cachex("cache1").cache().context().group().preloader().rebalanceFuture().get(getTestTimeout());

        stopGrid(2);

        grid.cluster().setBaselineTopology(4);

        grid.cluster().active(true);

        grid.cachex("cache1").cache().context().group().preloader().rebalanceFuture().get(getTestTimeout());

        assertPartitionsSame(idleVerify(grid, "cache1"));
    }

    private void checkIndexRows(IgniteEx ignite, List<Integer> clearedParts) {
        long cnt = 0L;

        for (int k = 0; k < NUM_KEYS/2; k++) {
            if (!clearedParts.contains(k % NUM_PARTS)) // ~ cctx.affinity().partition(k)
                cnt++;
        }

        checkIndexRows(ignite, "\"cache1\".TBL_TESTVALUEA", clearedParts, cnt);
        checkIndexRows(ignite, "\"cache1\".TBL_TESTVALUEB", clearedParts, cnt);
        checkIndexRows(ignite, "\"cache2\".TBL_TESTVALUEA", clearedParts, cnt);
        checkIndexRows(ignite, "\"cache2\".TBL_TESTVALUEB", clearedParts, cnt);
    }

    private void checkIndexRows(IgniteEx ignite, String schemaTable, List<Integer> clearedParts, long cnt) {
        List<List<?>> r = ignite.context().query().querySqlFields(
            new SqlFieldsQuery("SELECT COUNT(1) FROM " + schemaTable + " WHERE A > ? AND B > ?")
                .setLocal(true)
                .setArgs(Integer.toString(0), 0)
                .setPartitions(U.toIntArray(clearedParts))
            , false).getAll();

        assertEquals(0L, r.get(0).get(0));

        r = ignite.context().query().querySqlFields(
            new SqlFieldsQuery("SELECT COUNT(1) FROM " + schemaTable + " WHERE C > ?")
                .setLocal(true)
                .setArgs(Integer.toString(0))
                .setPartitions(U.toIntArray(clearedParts))
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
     * @throws Exception If failed.
     */
    @SuppressWarnings({"ConstantConditions", "ThrowableNotThrown"})
    @Test
    public void testCacheStopDuringPartitionSetEvict() throws Exception {
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

        List<GridDhtLocalPartition> parts = partIds.stream().map(id-> cctx.topology().localPartition(id))
            .collect(Collectors.toList());

        parts.forEach(GridDhtLocalPartition::reserve);

        startGrid(2);

        parts.forEach(GridDhtLocalPartition::moving);

        IgniteInternalFuture<Void> fut = cctx.shared().evict().evictPartitionsExclusively(cctx.group(), parts);

        parts.forEach(GridDhtLocalPartition::release);

        BlockPurgeIndexing blkIdx = (BlockPurgeIndexing)grid.context().query().getIndexing();

        blkIdx.waitPhase1();

        GridTestUtils.runAsync(() -> stopGrid(0));

        U.sleep(500);

        blkIdx.startPhase2();

        GridTestUtils.assertThrows(log, () -> fut.get(getTestTimeout()), IgniteCheckedException.class, "");
    }

    // TODO: Need recovery test to see if new delta record works
    // TODO: Is concurrent IndexRebuild and eviction possible? indexes not reported by GridH2Table while rebuilding any.
    // TODO: exclusivePartitionsEvict(grp, parts) on two different cache groups.
    // TODO: part.clearAsync() after exclusivePartitionsEvict(grp, parts)
    // TODO: exclusivePartitionsEvict(grp, parts) after part.clearAsync()
    // TODO: Simultaneous exclusive evict and regular clear on different partitions of the same cache group.
    // TODO: checks to prevent mvcc, dr, cq, cache-store, indexing spi, text index.

    /**
     *
     * @param ignite Ignite.
     * @throws Exception If failed.
     */
    private void fillData(IgniteEx ignite) throws Exception {
        Collection<String> caches = F.asList("cache1", "cache2", "cache3");

        ignite.context().cache().changeWalMode(caches, false);

        fillData(ignite, "cache1");
        fillData(ignite, "cache2");
        fillData(ignite, "cache3");

        forceCheckpoint(ignite);

        ignite.context().cache().changeWalMode(caches, true);
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
        private AtomicInteger count = new AtomicInteger();

        /** */
        CountDownLatch phase1 = new CountDownLatch(1);

        /** */
        CountDownLatch phase2 = new CountDownLatch(1);

        /** {@inheritDoc} */
        @Override public List<IgniteBiTuple<Runnable, IgniteInternalFuture<Void>>> purgeIndexPartitions(
            CacheGroupContext grp, Set<Integer> parts) {
            List<IgniteBiTuple<Runnable, IgniteInternalFuture<Void>>> res = super.purgeIndexPartitions(grp, parts);

            count.set(res.size());

            return res.stream().map(this::wrap).collect(Collectors.toList());
        }

        public void waitPhase1() {
            try {
                phase1.await(TIMEOUT, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        public void startPhase2() {
            phase2.countDown();
        }

        /**
         *
         * @param t
         * @return
         */
        private IgniteBiTuple<Runnable, IgniteInternalFuture<Void>> wrap(
            IgniteBiTuple<Runnable, IgniteInternalFuture<Void>> t) {

            return new IgniteBiTuple<>(() -> {
                if (count.decrementAndGet() == 0) {
                    phase1.countDown();

                    try {
                        phase2.await(TIMEOUT, TimeUnit.MILLISECONDS);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }

                t.get1().run();
            }, t.get2());
        }
    }
}
