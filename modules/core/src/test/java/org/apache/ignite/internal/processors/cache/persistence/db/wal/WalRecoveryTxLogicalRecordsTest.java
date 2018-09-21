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

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.IgniteRebalanceIterator;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtDemandedPartitionsMap;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointHistory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.freelist.AbstractFreeList;
import org.apache.ignite.internal.processors.cache.persistence.freelist.CacheFreeListImpl;
import org.apache.ignite.internal.processors.cache.persistence.freelist.PagesList;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseListImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Assert;

/**
 *
 */
public class WalRecoveryTxLogicalRecordsTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Cache 2 name. */
    private static final String CACHE2_NAME = "cache2";

    /** */
    public static final int PARTS = 32;

    /** */
    public static final int WAL_HIST_SIZE = 30;

    /** */
    private int pageSize = 4 * 1024;

    /** */
    private CacheConfiguration<?, ?> extraCcfg;

    /** */
    private Long checkpointFreq;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration<Integer, IndexedValue> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, PARTS));
        ccfg.setIndexedTypes(Integer.class, IndexedValue.class);

        if (extraCcfg != null)
            cfg.setCacheConfiguration(ccfg, new CacheConfiguration<>(extraCcfg));
        else
            cfg.setCacheConfiguration(ccfg);

        DataStorageConfiguration dbCfg = new DataStorageConfiguration();

        dbCfg.setPageSize(pageSize);

        dbCfg.setWalHistorySize(WAL_HIST_SIZE);

        dbCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration()
            .setMaxSize(100L * 1024 * 1024)
            .setPersistenceEnabled(true));

        if (checkpointFreq != null)
            dbCfg.setCheckpointFrequency(checkpointFreq);

        cfg.setDataStorageConfiguration(dbCfg);

        cfg.setMarshaller(null);

        BinaryConfiguration binCfg = new BinaryConfiguration();

        binCfg.setCompactFooter(false);

        cfg.setBinaryConfiguration(binCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    public void testWalTxSimple() throws Exception {
        Ignite ignite = startGrid();

        ignite.cluster().active(true);

        try {
            GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)((IgniteEx)ignite).context()
                .cache().context().database();

            dbMgr.enableCheckpoints(false).get();

            IgniteCache<Integer, IndexedValue> cache = ignite.cache(CACHE_NAME);

            int txCnt = 100;

            int keysPerTx = 10;

            for (int i = 0; i < txCnt; i++) {
                try (Transaction tx = ignite.transactions().txStart()) {
                    for (int j = 0; j < keysPerTx; j++) {
                        int k = i * keysPerTx + j;

                        cache.put(k, new IndexedValue(k));
                    }

                    tx.commit();
                }
            }

            for (int i = 0; i < txCnt; i++) {
                for (int j = 0; j < keysPerTx; j++) {
                    int k = i * keysPerTx + j;

                    assertEquals(k, cache.get(k).value());
                }
            }

            stopGrid();

            ignite = startGrid();

            ignite.cluster().active(true);

            cache = ignite.cache(CACHE_NAME);

            for (int i = 0; i < txCnt; i++) {
                for (int j = 0; j < keysPerTx; j++) {
                    int k = i * keysPerTx + j;

                    assertEquals(k, cache.get(k).value());
                }
            }

            for (int i = 0; i < txCnt; i++) {
                for (int j = 0; j < keysPerTx; j++) {
                    int k = i * keysPerTx + j;

                    QueryCursor<List<?>> cur = cache.query(
                        new SqlFieldsQuery("select sVal from IndexedValue where iVal=?").setArgs(k));

                    List<List<?>> vals = cur.getAll();

                    assertEquals(vals.size(), 1);
                    assertEquals("string-" + k, vals.get(0).get(0));
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testWalRecoveryRemoves() throws Exception {
        Ignite ignite = startGrid();

        ignite.cluster().active(true);

        try {
            GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)((IgniteEx)ignite).context()
                .cache().context().database();

            IgniteCache<Integer, IndexedValue> cache = ignite.cache(CACHE_NAME);

            int txCnt = 100;

            int keysPerTx = 10;

            for (int i = 0; i < txCnt; i++) {
                try (Transaction tx = ignite.transactions().txStart()) {
                    for (int j = 0; j < keysPerTx; j++) {
                        int k = i * keysPerTx + j;

                        cache.put(k, new IndexedValue(k));
                    }

                    tx.commit();
                }
            }

            for (int i = 0; i < txCnt; i++) {
                for (int j = 0; j < keysPerTx; j++) {
                    int k = i * keysPerTx + j;

                    assertEquals(k, cache.get(k).value());
                }
            }

            dbMgr.waitForCheckpoint("test");
            dbMgr.enableCheckpoints(false).get();

            for (int i = 0; i < txCnt / 2; i++) {
                try (Transaction tx = ignite.transactions().txStart()) {
                    for (int j = 0; j < keysPerTx; j++) {
                        int k = i * keysPerTx + j;

                        cache.remove(k);
                    }

                    tx.commit();
                }
            }

            stopGrid();

            ignite = startGrid();

            ignite.cluster().active(true);

            cache = ignite.cache(CACHE_NAME);

            for (int i = 0; i < txCnt; i++) {
                for (int j = 0; j < keysPerTx; j++) {
                    int k = i * keysPerTx + j;

                    QueryCursor<List<?>> cur = cache.query(
                        new SqlFieldsQuery("select sVal from IndexedValue where iVal=?").setArgs(k));

                    List<List<?>> vals = cur.getAll();

                    if (i < txCnt / 2) {
                        assertNull(cache.get(k));
                        assertTrue(F.isEmpty(vals));
                    }
                    else {
                        assertEquals(k, cache.get(k).value());

                        assertEquals(1, vals.size());
                        assertEquals("string-" + k, vals.get(0).get(0));
                    }
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testHistoricalRebalanceIterator() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD, "0");

        extraCcfg = new CacheConfiguration(CACHE_NAME + "2");
        extraCcfg.setAffinity(new RendezvousAffinityFunction(false, PARTS));

        Ignite ignite = startGrid();

        try {
            ignite.cluster().active(true);

            GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)((IgniteEx)ignite).context()
                .cache().context().database();

            dbMgr.waitForCheckpoint("test");

            // This number depends on wal history size.
            int entries = 25;

            IgniteCache<Integer, Integer> cache = ignite.cache(CACHE_NAME);
            IgniteCache<Integer, Integer> cache2 = ignite.cache(CACHE_NAME + "2");

            for (int i = 0; i < entries; i++) {
                // Put to partition 0.
                cache.put(i * PARTS, i * PARTS);

                // Put to partition 1.
                cache.put(i * PARTS + 1, i * PARTS + 1);

                // Put to another cache.
                cache2.put(i, i);

                dbMgr.waitForCheckpoint("test");
            }

            for (int i = 0; i < entries; i++) {
                assertEquals((Integer)(i * PARTS), cache.get(i * PARTS));
                assertEquals((Integer)(i * PARTS + 1), cache.get(i * PARTS + 1));
                assertEquals((Integer)(i), cache2.get(i));
            }

            CacheGroupContext grp = ((IgniteEx)ignite).context().cache().cacheGroup(CU.cacheId(CACHE_NAME));
            IgniteCacheOffheapManager offh = grp.offheap();
            AffinityTopologyVersion topVer = grp.affinity().lastVersion();

            IgniteDhtDemandedPartitionsMap map;

            for (int i = 0; i < entries; i++) {
                map = new IgniteDhtDemandedPartitionsMap();
                map.addHistorical(0, i, Long.MAX_VALUE, entries);

                try (IgniteRebalanceIterator it = offh.rebalanceIterator(map, topVer)) {
                    assertNotNull(it);

                    assertTrue("Not historical for iteration: " + i, it.historical(0));

                    for (int j = i; j < entries; j++) {
                        assertTrue("i=" + i + ", j=" + j, it.hasNextX());

                        CacheDataRow row = it.next();

                        assertEquals(j * PARTS, (int)row.key().value(grp.cacheObjectContext(), false));
                        assertEquals(j * PARTS, (int)row.value().value(grp.cacheObjectContext(), false));
                    }

                    assertFalse(it.hasNext());
                }

                map = new IgniteDhtDemandedPartitionsMap();
                map.addHistorical(1, i, Long.MAX_VALUE, entries);

                try (IgniteRebalanceIterator it = offh.rebalanceIterator(map, topVer)) {
                    assertNotNull(it);

                    assertTrue("Not historical for iteration: " + i, it.historical(1));

                    for (int j = i; j < entries; j++) {
                        assertTrue(it.hasNextX());

                        CacheDataRow row = it.next();

                        assertEquals(j * PARTS + 1, (int)row.key().value(grp.cacheObjectContext(), false));
                        assertEquals(j * PARTS + 1, (int)row.value().value(grp.cacheObjectContext(), false));
                    }

                    assertFalse(it.hasNext());
                }
            }

            stopAllGrids();

            // Check that iterator is valid after restart.
            ignite = startGrid();

            ignite.cluster().active(true);

            grp = ((IgniteEx)ignite).context().cache().cacheGroup(CU.cacheId(CACHE_NAME));
            offh = grp.offheap();
            topVer = grp.affinity().lastVersion();

            for (int i = 0; i < entries; i++) {
                long start = System.currentTimeMillis();

                map = new IgniteDhtDemandedPartitionsMap();
                map.addHistorical(0, i, Long.MAX_VALUE, entries);

                try (IgniteRebalanceIterator it = offh.rebalanceIterator(map, topVer)) {
                    long end = System.currentTimeMillis();

                    info("Time to get iterator: " + (end - start));

                    assertTrue("Not historical for iteration: " + i, it.historical(0));

                    assertNotNull(it);

                    start = System.currentTimeMillis();

                    for (int j = i; j < entries; j++) {
                        assertTrue("i=" + i + ", j=" + j, it.hasNextX());

                        CacheDataRow row = it.next();

                        assertEquals(j * PARTS, (int)row.key().value(grp.cacheObjectContext(), false));
                        assertEquals(j * PARTS, (int)row.value().value(grp.cacheObjectContext(), false));
                    }

                    end = System.currentTimeMillis();

                    info("Time to iterate: " + (end - start));

                    assertFalse(it.hasNext());
                }

                map = new IgniteDhtDemandedPartitionsMap();
                map.addHistorical(1, i, Long.MAX_VALUE, entries);

                try (IgniteRebalanceIterator it = offh.rebalanceIterator(map, topVer)) {
                    assertNotNull(it);

                    assertTrue("Not historical for iteration: " + i, it.historical(1));

                    for (int j = i; j < entries; j++) {
                        assertTrue(it.hasNextX());

                        CacheDataRow row = it.next();

                        assertEquals(j * PARTS + 1, (int)row.key().value(grp.cacheObjectContext(), false));
                        assertEquals(j * PARTS + 1, (int)row.value().value(grp.cacheObjectContext(), false));
                    }

                    assertFalse(it.hasNext());
                }
            }
        }
        finally {
            stopAllGrids();

            System.clearProperty(IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testWalAfterPreloading() throws Exception {
        Ignite ignite = startGrid();

        ignite.cluster().active(true);

        try {
            GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)((IgniteEx)ignite).context()
                .cache().context().database();

            dbMgr.enableCheckpoints(false).get();

            int entries = 100;

            try (IgniteDataStreamer<Integer, Integer> streamer = ignite.dataStreamer(CACHE_NAME)) {
                for (int i = 0; i < entries; i++)
                    streamer.addData(i, i);
            }

            IgniteCache<Integer, Integer> cache = ignite.cache(CACHE_NAME);

            for (int i = 0; i < entries; i++)
                assertEquals(new Integer(i), cache.get(i));

            stopGrid();

            ignite = startGrid();

            ignite.cluster().active(true);

            cache = ignite.cache(CACHE_NAME);

            for (int i = 0; i < entries; i++)
                assertEquals(new Integer(i), cache.get(i));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRecoveryRandomPutRemove() throws Exception {
        try {
            pageSize = 1024;

            extraCcfg = new CacheConfiguration(CACHE2_NAME);
            extraCcfg.setAffinity(new RendezvousAffinityFunction(false, PARTS));

            Ignite ignite = startGrid(0);

            ignite.cluster().active(true);

            GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)((IgniteEx)ignite).context()
                .cache().context().database();

            dbMgr.enableCheckpoints(false).get();

            IgniteCache<Integer, IndexedValue> cache1 = ignite.cache(CACHE_NAME);
            IgniteCache<Object, Object> cache2 = ignite.cache(CACHE2_NAME);

            final int KEYS1 = 100;

            for (int i = 0; i < KEYS1; i++)
                cache1.put(i, new IndexedValue(i));

            for (int i = 0; i < KEYS1; i++) {
                if (i % 2 == 0)
                    cache1.remove(i);
            }

            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            for (int i = 0; i < KEYS1; i++) {
                cache2.put(i, new byte[rnd.nextInt(512)]);

                if (rnd.nextBoolean())
                    cache2.put(i, new byte[rnd.nextInt(512)]);

                if (rnd.nextBoolean())
                    cache2.remove(i);
            }

            ignite.close();

            ignite = startGrid(0);

            ignite.cluster().active(true);

            ignite.cache(CACHE_NAME).put(1, new IndexedValue(0));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRecoveryNoPageLost1() throws Exception {
        recoveryNoPageLost(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRecoveryNoPageLost2() throws Exception {
        recoveryNoPageLost(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRecoveryNoPageLost3() throws Exception {
        try {
            pageSize = 1024;
            checkpointFreq = 100L;
            extraCcfg = new CacheConfiguration(CACHE2_NAME);
            extraCcfg.setAffinity(new RendezvousAffinityFunction(false, 32));

            List<Integer> pages = null;

            for (int iter = 0; iter < 5; iter++) {
                log.info("Start node: " + iter);

                Ignite ignite = startGrid(0);

                ignite.cluster().active(true);

                if (pages != null) {
                    List<Integer> curPags = allocatedPages(ignite, CACHE2_NAME);

                    assertEquals("Iter = " + iter, pages, curPags);
                }

                final IgniteCache<Integer, Object> cache = ignite.cache(CACHE2_NAME);

                final int ops = ThreadLocalRandom.current().nextInt(10) + 10;

                GridTestUtils.runMultiThreaded(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        ThreadLocalRandom rnd = ThreadLocalRandom.current();

                        for (int i = 0; i < ops; i++) {
                            Integer key = rnd.nextInt(1000);

                            cache.put(key, new byte[rnd.nextInt(512)]);

                            if (rnd.nextBoolean())
                                cache.remove(key);
                        }

                        return null;
                    }
                }, 10, "update");

                pages = allocatedPages(ignite, CACHE2_NAME);

                Ignition.stop(ignite.name(), false); //will make checkpoint
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param checkpoint Checkpoint enable flag.
     * @throws Exception If failed.
     */
    private void recoveryNoPageLost(boolean checkpoint) throws Exception {
        try {
            pageSize = 1024;
            extraCcfg = new CacheConfiguration(CACHE2_NAME);
            extraCcfg.setAffinity(new RendezvousAffinityFunction(false, 32));

            List<Integer> pages = null;

            AtomicInteger cnt = new AtomicInteger();

            for (int iter = 0; iter < 5; iter++) {
                log.info("Start node: " + iter);

                Ignite ignite = startGrid(0);

                ignite.cluster().active(true);

                GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)((IgniteEx)ignite).context()
                    .cache().context().database();

                if (!checkpoint)
                    dbMgr.enableCheckpoints(false).get();

                if (pages != null) {
                    List<Integer> curPags = allocatedPages(ignite, CACHE2_NAME);

                    assertEquals(pages, curPags);
                }

                IgniteCache<Integer, Object> cache = ignite.cache(CACHE2_NAME);

                for (int i = 0; i < 128; i++)
                    cache.put(cnt.incrementAndGet(), new byte[256 + iter * 100]);

                pages = allocatedPages(ignite, CACHE2_NAME);

                ignite.close();
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param ignite Node.
     * @param cacheName Cache name.
     * @return Allocated pages per-store.
     * @throws Exception If failed.
     */
    private List<Integer> allocatedPages(Ignite ignite, String cacheName) throws Exception {
        FilePageStoreManager storeMgr =
            (FilePageStoreManager)((IgniteEx)ignite).context().cache().context().pageStore();

        int parts = ignite.affinity(cacheName).partitions();

        List<Integer> res = new ArrayList<>(parts);

        for (int p = 0; p < parts; p++) {
            PageStore store = storeMgr.getStore(CU.cacheId(cacheName), p);

            store.sync();

            res.add(store.pages());
        }

        PageStore store = storeMgr.getStore(CU.cacheId(cacheName), PageIdAllocator.INDEX_PARTITION);

        store.sync();

        res.add(store.pages());

        return res;
    }

    /**
     * @throws Exception If failed.
     */
    public void testFreeListRecovery() throws Exception {
        try {
            pageSize = 1024;
            extraCcfg = new CacheConfiguration(CACHE2_NAME);

            Ignite ignite = startGrid(0);

            ignite.cluster().active(true);

            IgniteCache<Integer, IndexedValue> cache1 = ignite.cache(CACHE_NAME);
            IgniteCache<Object, Object> cache2 = ignite.cache(CACHE2_NAME);

            final int KEYS1 = 2048;

            for (int i = 0; i < KEYS1; i++)
                cache1.put(i, new IndexedValue(i));

            for (int i = 0; i < KEYS1; i++) {
                if (i % 2 == 0)
                    cache1.remove(i);
            }

            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            for (int i = 0; i < KEYS1; i++) {
                cache2.put(i, new byte[rnd.nextInt(512)]);

                if (rnd.nextBoolean())
                    cache2.put(i, new byte[rnd.nextInt(512)]);

                if (rnd.nextBoolean())
                    cache2.remove(i);
            }

            Map<Integer, T2<Map<Integer, long[]>, int[]>> cache1_1 = getFreeListData(ignite, CACHE_NAME);
            Map<Integer, T2<Map<Integer, long[]>, int[]>> cache2_1 = getFreeListData(ignite, CACHE2_NAME);
            T2<long[], Integer> rl1_1 = getReuseListData(ignite, CACHE_NAME);
            T2<long[], Integer> rl2_1 = getReuseListData(ignite, CACHE2_NAME);

            ignite.close();

            ignite = startGrid(0);

            ignite.cluster().active(true);

            cache1 = ignite.cache(CACHE_NAME);
            cache2 = ignite.cache(CACHE2_NAME);

            for (int i = 0; i < KEYS1; i++) {
                cache1.get(i);
                cache2.get(i);
            }

            Map<Integer, T2<Map<Integer, long[]>, int[]>> cache1_2 = getFreeListData(ignite, CACHE_NAME);
            Map<Integer, T2<Map<Integer, long[]>, int[]>> cache2_2 = getFreeListData(ignite, CACHE2_NAME);
            T2<long[], Integer> rl1_2 = getReuseListData(ignite, CACHE_NAME);
            T2<long[], Integer> rl2_2 = getReuseListData(ignite, CACHE2_NAME);

            checkEquals(cache1_1, cache1_2);
            checkEquals(cache2_1, cache2_2);
            checkEquals(rl1_1, rl1_2);
            checkEquals(rl2_1, rl2_2);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param ignite Node.
     * @param cacheName Cache name.
     * @return Cache reuse list data.
     */
    private T2<long[], Integer> getReuseListData(Ignite ignite, String cacheName) {
        GridCacheContext ctx = ((IgniteEx)ignite).context().cache().cache(cacheName).context();

        ReuseListImpl reuseList = GridTestUtils.getFieldValue(ctx.offheap(), "reuseList");
        PagesList.Stripe[] bucket = GridTestUtils.getFieldValue(reuseList, "bucket");

        long[] ids = null;

        if (bucket != null) {
            ids = new long[bucket.length];

            for (int i = 0; i < bucket.length; i++)
                ids[i] = bucket[i].tailId;
        }

//        AtomicIntegerArray cnts = GridTestUtils.getFieldValue(reuseList, PagesList.class, "cnts");
//        assertEquals(1, cnts.length());

        return new T2<>(ids, 0);
    }

    /**
     * @param rl1 Data 1 (before stop).
     * @param rl2 Data 2 (after restore).
     */
    private void checkEquals(T2<long[], Integer> rl1, T2<long[], Integer> rl2) {
        Assert.assertArrayEquals(rl1.get1(), rl2.get1());
        assertEquals(rl1.get2(), rl2.get2());
    }

    /**
     * @param partsLists1 Data 1 (before stop).
     * @param partsLists2 Data 2 (after restore).
     */
    private void checkEquals(Map<Integer, T2<Map<Integer, long[]>, int[]>> partsLists1,
        Map<Integer, T2<Map<Integer, long[]>, int[]>> partsLists2) {
        assertEquals(partsLists1.size(), partsLists2.size());

        for (Integer part : partsLists1.keySet()) {
            T2<Map<Integer, long[]>, int[]> t1 = partsLists1.get(part);
            T2<Map<Integer, long[]>, int[]> t2 = partsLists2.get(part);

            Map<Integer, long[]> m1 = t1.get1();
            Map<Integer, long[]> m2 = t2.get1();

            assertEquals(m1.size(), m2.size());

            for (Integer bucket : m1.keySet()) {
                long tails1[] = m1.get(bucket);
                long tails2[] = m2.get(bucket);

                Assert.assertArrayEquals(tails1, tails2);
            }

            Assert.assertArrayEquals("Wrong counts [part=" + part + ']', t1.get2(), t2.get2());
        }
    }

    /**
     * @param ignite Node.
     * @param cacheName Cache name.
     * @return Cache free lists data.
     */
    private Map<Integer, T2<Map<Integer, long[]>, int[]>> getFreeListData(Ignite ignite, String cacheName) {
        GridCacheContext ctx = ((IgniteEx)ignite).context().cache().cache(cacheName).context();

        List<GridDhtLocalPartition> parts = ctx.topology().localPartitions();

        assertTrue(!parts.isEmpty());
        assertEquals(ctx.affinity().partitions(), parts.size());

        Map<Integer, T2<Map<Integer, long[]>, int[]>> res = new HashMap<>();

        boolean foundNonEmpty = false;
        boolean foundTails = false;

        for (GridDhtLocalPartition part : parts) {
            CacheFreeListImpl freeList = GridTestUtils.getFieldValue(part.dataStore(), "freeList");

            if (freeList == null)
                // Lazy store.
                continue;

            AtomicReferenceArray<PagesList.Stripe[]> buckets = GridTestUtils.getFieldValue(freeList,
                AbstractFreeList.class, "buckets");
            //AtomicIntegerArray cnts = GridTestUtils.getFieldValue(freeList, PagesList.class, "cnts");

            assertNotNull(buckets);
            //assertNotNull(cnts);
            assertTrue(buckets.length() > 0);
            //assertEquals(cnts.length(), buckets.length());

            Map<Integer, long[]> tailsPerBucket = new HashMap<>();

            for (int i = 0; i < buckets.length(); i++) {
                PagesList.Stripe[] tails = buckets.get(i);

                long ids[] = null;

                if (tails != null) {
                    ids = new long[tails.length];

                    for (int j = 0; j < tails.length; j++)
                        ids[j] = tails[j].tailId;
                }

                tailsPerBucket.put(i, ids);

                    if (tails != null) {
                        assertTrue(tails.length > 0);

                        foundTails = true;
                    }
                }

//            int[] cntsPerBucket = new int[cnts.length()];
//
//            for (int i = 0; i < cnts.length(); i++) {
//                cntsPerBucket[i] = cnts.get(i);
//
//                if (cntsPerBucket[i] > 0)
//                    foundNonEmpty = true;
//            }

            res.put(part.id(), new T2<>(tailsPerBucket, (int[])null));
        }

        //assertTrue(foundNonEmpty);
        assertTrue(foundTails);

        return res;
    }

    /**
     *
     */
    private static class IndexedValue {
        /** */
        @QuerySqlField(index = true)
        private int iVal;

        /** */
        @QuerySqlField
        private String sVal;

        /**
         * @param iVal Indexed value.
         */
        private IndexedValue(int iVal) {
            this.iVal = iVal;
            sVal = "string-" + iVal;
        }

        /**
         * @return Value.
         */
        private int value() {
            return iVal;
        }
    }
}
