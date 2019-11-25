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
package org.apache.ignite.internal.processors.query.h2;

import java.util.Collections;
import java.util.Objects;
import java.util.Random;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.verify.ValidateIndexesClosure;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesJobResult;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 *
 */
public class GridIndexPartitionRebuildTest extends GridCommonAbstractTest {
    /** */
    private static final String FIRST_CACHE = "cache1";

    /** */
    private static final String SECOND_CACHE = "cache2";

    /** */
    private static final String THIRD_CACHE = "cache3";

    /** */
    private static final int COUNT = 5_000;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        cleanPersistenceDir();

        super.afterTestsStopped();

        GridQueryProcessor.idxCls = null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        dsCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration()
            .setMaxSize(256 * 1024 * 1024)
            .setPersistenceEnabled(true)
        );

        dsCfg.setCheckpointFrequency(3_000);

        cfg.setDataStorageConfiguration(dsCfg);

        CacheConfiguration ccfg1 = new CacheConfiguration();

        ccfg1.setName(FIRST_CACHE)
            .setBackups(2)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(CacheMode.PARTITIONED)
            .setIndexedTypes(Integer.class, ValueA.class, Integer.class, ValueB.class);

        CacheConfiguration ccfg2 = new CacheConfiguration(ccfg1)
            .setName(SECOND_CACHE)
            .setGroupName("group");

        CacheConfiguration ccfg3 = new CacheConfiguration(ccfg2)
            .setName(THIRD_CACHE);

        cfg.setCacheConfiguration(ccfg1, ccfg2, ccfg3);

        return cfg;
    }

    /**
     * Rebuild single partition of a cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRebuildPartition() throws Exception {
        IgniteEx grid = startGrid(0);

        GridQueryProcessor.idxCls = TestIndexing.class;

        startGrid(1);

        grid.cluster().active(true);

        awaitPartitionMapExchange();

        CacheGroupContext grp = grid(1).context().cache().cache(FIRST_CACHE).context().group();

        Integer partId = grp.topology().localPartitions().stream().map(GridDhtLocalPartition::id).findFirst().get();

        getTestIndexing(grid(1)).ignoreParts.add(partId);

        fillData(grid, FIRST_CACHE, COUNT);

        getTestIndexing(grid(1)).ignoreParts.remove(partId);

        IgniteInternalFuture putsFut = doBackgroundPuts(grid, FIRST_CACHE, COUNT/2);

        IgniteInternalFuture fut = grid(1).context().query().rebuildIndexesByPartition(grp, partId);

        fut.get();

        putsFut.cancel();

        forceCheckpoint(grid(1));

        validateIndexes(grid(1), FIRST_CACHE);
    }

    /**
     * Test that rebuild starts over after restart if node stops.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStopAndRebuild() throws Exception {
        IgniteEx grid = startGrid(0);

        GridQueryProcessor.idxCls = TestIndexing.class;

        startGrid(1);

        grid.cluster().active(true);

        awaitPartitionMapExchange();

        CacheGroupContext grp = grid(1).context().cache().cache(FIRST_CACHE).context().group();

        Integer partId = grp.topology().localPartitions().stream().map(GridDhtLocalPartition::id).findFirst().get();

        getTestIndexing(grid(1)).ignoreParts.add(partId);

        fillData(grid, FIRST_CACHE, COUNT);

        fillPartData(grid, FIRST_CACHE, partId, COUNT);

        getTestIndexing(grid(1)).ignoreParts.remove(partId);

        IgniteInternalFuture putsFut = doBackgroundPuts(grid, FIRST_CACHE, COUNT/2);

        grid(1).context().query().rebuildIndexesByPartition(grp, partId);

        U.sleep(500);

        stopAndCancelGrid(1);

        putsFut.cancel();

        startGrid(1);

        awaitPartitionMapExchange();

        IgniteFuture<?> idxReadyFut = grid(1).cache(FIRST_CACHE).indexReadyFuture();

        idxReadyFut.get();

        forceCheckpoint(grid(1));

        validateIndexes(grid(1), FIRST_CACHE);
    }

    /**
     * Test rebuild single partition of a cache group.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRebuildGroupPartition() throws Exception {
        IgniteEx grid = startGrid(0);

        GridQueryProcessor.idxCls = TestIndexing.class;

        startGrid(1);

        grid.cluster().active(true);

        awaitPartitionMapExchange();

        CacheGroupContext grp = grid(1).context().cache().cache(SECOND_CACHE).context().group();

        Integer partId = grp.topology().localPartitions().stream().map(GridDhtLocalPartition::id).findFirst().get();

        getTestIndexing(grid(1)).ignoreParts.add(partId);

        fillData(grid, SECOND_CACHE, COUNT);
        fillData(grid, THIRD_CACHE, COUNT);

        fillPartData(grid, SECOND_CACHE, partId, COUNT);
        fillPartData(grid, THIRD_CACHE, partId, COUNT);

        getTestIndexing(grid(1)).ignoreParts.remove(partId);

        IgniteInternalFuture putsFut1 = doBackgroundPuts(grid, SECOND_CACHE, COUNT/2);
        IgniteInternalFuture putsFut2 = doBackgroundPuts(grid, THIRD_CACHE, COUNT/2);

        IgniteInternalFuture fut = grid(1).context().query().rebuildIndexesByPartition(grp, partId);

        fut.get();

        putsFut1.cancel();
        putsFut2.cancel();

        forceCheckpoint(grid(1));

        validateIndexes(grid(1), SECOND_CACHE);
        validateIndexes(grid(1), THIRD_CACHE);
    }

    /**
     *
     * @param ignite Ignite.
     * @param cacheName Cache name.
     * @param cnt Count.
     */
    private void fillData(IgniteEx ignite, String cacheName, int cnt) {
        Random rnd = new Random();

        for (int idx = 0; idx < cnt; idx++) {
            ignite.cache(cacheName).put(idx, new ValueA((long)idx, rnd.nextInt(cnt)));
            ignite.cache(cacheName).put(idx, new ValueB((long)idx, rnd.nextInt(cnt)));
        }
    }

    /**
     *
     * @param ignite Ignite.
     * @param cacheName Cache name.
     * @param cnt Count.
     */
    private void fillPartData(IgniteEx ignite, String cacheName, int part, int cnt) {
        Random rnd = new Random();

        IgniteCache cache = ignite.cache(cacheName);

        Affinity aff = ignite.context().cache().cache(cacheName).affinity();

        int put = 0;

        for (int idx = 0; put < cnt; idx++) {
            if (part == aff.partition(idx)) {
                cache.put(idx, new ValueA((long)idx, rnd.nextInt(cnt)));
                cache.put(idx, new ValueB((long)idx, rnd.nextInt(cnt)));

                put++;
            }
        }
    }

    /**
     *
     * @param ignite Ignite.
     * @param start First key to create.
     * @return future/
     */
    private IgniteInternalFuture<?> doBackgroundPuts(IgniteEx ignite, String cacheName, int start) {
        return GridTestUtils.runAsync(()-> {
            Random rnd = new Random();
            int i = start;

            while (!Thread.currentThread().isInterrupted()) {
                ignite.cache(cacheName).put(i, new ValueA((long)i, rnd.nextInt(start)));

                ignite.cache(cacheName).put(i, new ValueB((long)i, rnd.nextInt(start)));

                i++;
            }
        });
    }

    /**
     *
     * @param ignite Ignite.
     * @return Indexing instance.
     */
    private TestIndexing getTestIndexing(IgniteEx ignite) {
        IgniteH2Indexing idx = (IgniteH2Indexing)ignite.context().query().getIndexing();

        assertTrue(idx instanceof TestIndexing);

        return ((TestIndexing)idx);
    }

    /**
     *
     * @param ignite Ignite.
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void validateIndexes(IgniteEx ignite, String cacheName) throws Exception {
        ValidateIndexesClosure clo = new ValidateIndexesClosure(Collections.singleton(cacheName), 0, 1);

        ignite.context().resource().injectGeneric(clo);

        VisorValidateIndexesJobResult res = clo.call();

        assertFalse(res.hasIssues());
    }

    /**
     * Value for the first table.
     */
    public class ValueA {
        /** */
        private Long id;

        /** */
        @QuerySqlField(index = true)
        private int val;

        /** */
        @QuerySqlField(index = true)
        protected String name;

        /**
         *
         * @param id ID.
         * @param val Value.
         */
        public ValueA(Long id, int val) {
            this.id = id;

            this.val = val;

            name = "A" + id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            ValueA val = (ValueA)o;
            return Objects.equals(id, val.id);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(id) ^ val;
        }
    }

    /**
     * Value for the second table.
     */
    public class ValueB extends ValueA {
        /**
         *
         * @param id ID.
         * @param val Value.
         */
        public ValueB(Long id, int val) {
            super(id, val);

            name = "B" + id;
        }
    }

    /**
     * Indexing processor.
     */
    private static class TestIndexing extends IgniteH2Indexing {
        /** */
        private GridConcurrentHashSet<Integer> ignoreParts = new GridConcurrentHashSet<>();

        /** {@inheritDoc} */
        @Override public void store(GridCacheContext cctx, GridQueryTypeDescriptor type, CacheDataRow row,
            @Nullable CacheDataRow prevRow, boolean prevRowAvailable) throws IgniteCheckedException {

            if (!ignoreParts.contains(row.partition()))
                super.store(cctx, type, row, prevRow, prevRowAvailable);
        }
    }
}
