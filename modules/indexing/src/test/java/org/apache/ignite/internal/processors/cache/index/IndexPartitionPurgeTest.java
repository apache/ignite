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

import java.util.Collection;
import java.util.List;

import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IndexPartitionPurgeTest extends GridCommonAbstractTest {
    /** */
    private final static int NUM_KEYS = 50_000;

    /** */
    private final static long TIMEOUT = 600_000L;

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

        igniteCfg.setConsistentId(igniteInstanceName);

        igniteCfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(true).setMaxSize(4L*1024 * 1024 * 1024)
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
            .setAffinity(new RendezvousAffinityFunction().setPartitions(10))
            .setBackups(1)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setCacheMode(CacheMode.PARTITIONED)
            .setQueryEntities(F.asList(
                    getQueryEntity(TestValueA.class),
                    getQueryEntity(TestValueB.class)))
            .setSqlIndexMaxInlineSize(20)
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
    public void testPartitionSetEvict() throws Exception {
        startGrids(2);

        IgniteEx grid = grid(0);

        grid.cluster().active(true);

        awaitPartitionMapExchange();

        fillData(grid);

        IgniteCache cache = grid.cache("cache1");

        GridCacheContext cctx = grid.cachex("cache1").context();

        List<Integer> partIds = evictingPartitionsAfterJoin(grid, cache, 10);

        List<GridDhtLocalPartition> parts = partIds.stream().map(id-> cctx.topology().localPartition(id))
            .collect(Collectors.toList());

        parts.forEach(p -> p.reserve());

        startGrid(2);

        parts.forEach(p -> p.moving());

        IgniteInternalFuture<Void> fut = cctx.shared().evict().evictPartitionsExclusively(cctx.group(), parts);

        parts.forEach(p -> p.release());

        fut.get(getTestTimeout());

        for (GridDhtLocalPartition part : parts) {
            assertTrue(part.markForDestroy());

            part.setState(GridDhtPartitionState.EVICTED);

            part.destroy();

            part.awaitDestroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheStopDuringPartitionSetEvict() throws Exception {
        startGrids(2);

        IgniteEx grid = grid(0);

        grid.cluster().active(true);

        awaitPartitionMapExchange();

        fillData(grid);

        IgniteCache cache = grid.cache("cache1");

        GridCacheContext cctx = grid.cachex("cache1").context();

        List<Integer> partIds = evictingPartitionsAfterJoin(grid, cache, 10);

        List<GridDhtLocalPartition> parts = partIds.stream().map(id-> cctx.topology().localPartition(id))
            .collect(Collectors.toList());

        parts.forEach(p -> p.reserve());

        startGrid(2);

        parts.forEach(p -> p.moving());

        IgniteInternalFuture<Void> fut = cctx.shared().evict().evictPartitionsExclusively(cctx.group(), parts);

        parts.forEach(p -> p.release());

        stopGrid(0);

        try {
            fut.get(getTestTimeout());

            fail("must throw");
        }
        catch (Throwable e) {
        }
    }

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
}
