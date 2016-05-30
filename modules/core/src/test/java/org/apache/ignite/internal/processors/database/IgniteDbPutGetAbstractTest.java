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

package org.apache.ignite.internal.processors.database;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DatabaseConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.util.GridRandom;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public abstract class IgniteDbPutGetAbstractTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /**
     * @return Node count.
     */
    protected abstract int gridCount();

    /**
     * @return {@code True} if indexing is enabled.
     */
    protected abstract boolean indexingEnabled();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        DatabaseConfiguration dbCfg = new DatabaseConfiguration();

        dbCfg.setConcurrencyLevel(Runtime.getRuntime().availableProcessors() * 4);

        dbCfg.setPageSize(1024);

        dbCfg.setPageCacheSize(100 * 1024 * 1024);

        cfg.setDatabaseConfiguration(dbCfg);

        CacheConfiguration ccfg = new CacheConfiguration();

        if (indexingEnabled())
            ccfg.setIndexedTypes(Integer.class, DbValue.class);

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);

        CacheConfiguration ccfg2 = new CacheConfiguration("non-primitive");

        if (indexingEnabled())
            ccfg2.setIndexedTypes(DbKey.class, DbValue.class);

        ccfg2.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg2.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg2.setRebalanceMode(CacheRebalanceMode.SYNC);

        cfg.setCacheConfiguration(ccfg, ccfg2);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);
        cfg.setMarshaller(null);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        long seed = 1464583813940L; // System.currentTimeMillis();

        info("Seed: " + seed + "L");

        BPlusTree.rnd = new Random(seed);

        startGrids(gridCount());

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        BPlusTree.rnd = null;

        stopAllGrids();
    }

    public void testGradualRandomPutAllRemoveAll() {
        IgniteEx ig = grid(0);

        IgniteCache<Integer, DbValue> cache = ig.cache(null);

        final int cnt = 100_000;

        Random rnd = BPlusTree.rnd;

        assert rnd != null;

        Map<Integer, DbValue> map = new HashMap<>();

        for (int i = 0; i < 5; i++) {
            info("Iteration: " + i);

            info("Grow...");

            while (map.size() < cnt / 2)
                doPutRemoveAll(rnd, cache, map, cnt, true);

            info("Shrink...");

            while (map.size() > cnt / 4)
                doPutRemoveAll(rnd, cache, map, cnt, false);

            info("Check...");

            for (Integer key : map.keySet())
                assertEquals(map.get(key), cache.get(key));
        }
    }

    private void doPutRemoveAll(Random rnd, IgniteCache<Integer, DbValue> cache, Map<Integer, DbValue> map,
        int keysCnt, boolean grow) {
        int putCnt = grow ? 20 + rnd.nextInt(10) : 1 + rnd.nextInt(5);
        int rmvCnt = grow ? 1 + rnd.nextInt(5) : 20 + rnd.nextInt(10);

        Map<Integer, DbValue> put = new HashMap<>(putCnt);

        for (int i = 0; i < putCnt; i++) {
            int k = rnd.nextInt(keysCnt);

            put.put(k, new DbValue(rnd.nextInt(500), rnd.nextInt(500) + "-value", i));
        }

        map.putAll(put);
        cache.putAll(put);

        Set<Integer> rmv = new HashSet<>();

        for (int i = 0; i < rmvCnt; i++) {
            int k = rnd.nextInt(keysCnt);

            rmv.add(k);
            map.remove(k);
        }

        cache.removeAll(rmv);
    }

    /**
     *
     */
    public void testRandomRemove() {
        IgniteEx ig = grid(0);

        IgniteCache<Integer, DbValue> cache = ig.cache(null);

        final int cnt = 50_000;

        long seed = System.nanoTime();

        X.println("Seed: " + seed);

        Random rnd = new GridRandom(seed);

        int[] keys = generateUniqueRandomKeys(cnt, rnd);

        X.println("Put start");

        for (int i : keys) {
            DbValue v0 = new DbValue(i, "test-value", i);

//            if (i % 1000 == 0)
//                X.println(" --> " + i);

            cache.put(i, v0);

            assertEquals(v0, cache.get(i));
        }

        keys = generateUniqueRandomKeys(cnt, rnd);

        X.println("Rmv start");

        for (int i : keys) {
//            X.println(" --> " + i);

            assertTrue(cache.remove(i));
        }
    }


    /**
     */
    public void testRandomPut() {
        IgniteEx ig = grid(0);

        IgniteCache<Integer, DbValue> cache = ig.cache(null);

        final int cnt = 1_000;

        long seed = System.nanoTime();

        X.println("Seed: " + seed);

        Random rnd = new GridRandom(seed);

        for (int i = 0; i < 500_000; i++) {
            int k = rnd.nextInt(cnt);

            DbValue v0 = new DbValue(k, "test-value " + k, i);

            if (i % 1000 == 0)
                X.println(" --> " + i);

            cache.put(k, v0);

            assertEquals(v0, cache.get(k));
        }
    }


    /**
     * @throws Exception if failed.
     */
    public void testPutGetSimple() throws Exception {
        IgniteEx ig = grid(0);

        IgniteCache<Integer, DbValue> cache = ig.cache(null);

        GridCacheAdapter<Object, Object> internalCache = ig.context().cache().internalCache();

        int k0 = 0;
        DbValue v0 = new DbValue(0, "value-0", 0L);

        cache.put(k0, v0);

        checkEmpty(internalCache, k0);

        assertEquals(v0, cache.get(k0));

        checkEmpty(internalCache, k0);

        assertEquals(v0, cache.get(k0));

        checkEmpty(internalCache, k0);
    }

    /**
     * @throws Exception if failed.
     */
    public void testPutGetOverwrite() throws Exception {
        IgniteEx ig = grid(0);

        final IgniteCache<Integer, DbValue> cache = ig.cache(null);

        GridCacheAdapter<Object, Object> internalCache = ig.context().cache().internalCache();

        final int k0 = 0;
        DbValue v0 = new DbValue(0, "value-0", 0L);

        cache.put(k0, v0);

        checkEmpty(internalCache, k0);

        assertEquals(v0, cache.get(k0));

        checkEmpty(internalCache, k0);

        DbValue v1 = new DbValue(1, "value-1", 1L);

        cache.put(k0, v1);

        checkEmpty(internalCache, k0);

        assertEquals(v1, cache.get(k0));
    }

    /**
     * @throws Exception if failed.
     */
    public void testOverwriteNormalSizeAfterSmallerSize() throws Exception {
        IgniteEx ig = grid(0);

        final IgniteCache<Integer, DbValue> cache = ig.cache(null);

        GridCacheAdapter<Object, Object> internalCache = ig.context().cache().internalCache();

        String[] vals = new String[] {"long-long-long-value", "short-value"};
        final int k0 = 0;

        for (int i = 0; i < 10; i++) {
            DbValue v0 = new DbValue(i, vals[i % vals.length], i);

            info("Update.... " + i);

            cache.put(k0, v0);

            checkEmpty(internalCache, k0);

            assertEquals(v0, cache.get(k0));
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testPutDoesNotTriggerRead() throws Exception {
        IgniteEx ig = grid(0);

        final IgniteCache<Integer, DbValue> cache = ig.cache(null);

        cache.put(0, new DbValue(0, "test-value-0", 0));
    }

    /**
     * @throws Exception if failed.
     */
    public void testPutGetMultipleObjects() throws Exception {
        IgniteEx ig = grid(0);

        final IgniteCache<Integer, DbValue> cache = ig.cache(null);

        GridCacheAdapter<Object, Object> internalCache = ig.context().cache().internalCache();

        int cnt = 20_000;

        X.println("Put start");

        for (int i = 0; i < cnt; i++) {
            DbValue v0 = new DbValue(i, "test-value", i);

//            if (i % 1000 == 0)
//                X.println(" --> " + i);

            cache.put(i, v0);

            checkEmpty(internalCache, i);

            assertEquals(v0, cache.get(i));
        }

        X.println("Get start");

        for (int i = 0; i < cnt; i++) {
            DbValue v0 = new DbValue(i, "test-value", i);

            checkEmpty(internalCache, i);

//            X.println(" <-- " + i);

            assertEquals(v0, cache.get(i));
        }

        if (indexingEnabled()) {
            awaitPartitionMapExchange();

            X.println("Query start");

            assertEquals(cnt, cache.query(new SqlFieldsQuery("select null from dbvalue")).getAll().size());

            List<List<?>> res = cache.query(new SqlFieldsQuery("select ival, _val from dbvalue where ival < ?")
                .setArgs(10_000)).getAll();

            assertEquals(10_000, res.size());

            for (int i = 0; i < 10_000; i++) {
                List<?> row = res.get(i);

                assertEquals(2, row.size());
                assertEquals(i, row.get(0));

                assertEquals(new DbValue(i, "test-value", i), row.get(1));
            }

            assertEquals(1, cache.query(new SqlFieldsQuery("select lval from dbvalue where ival = 7899")).getAll().size());
            assertEquals(2000, cache.query(new SqlFieldsQuery("select lval from dbvalue where ival >= 5000 and ival < 7000"))
                .getAll().size());

            String plan = cache.query(new SqlFieldsQuery(
                "explain select lval from dbvalue where ival >= 5000 and ival < 7000")).getAll().get(0).get(0).toString();

            assertTrue(plan, plan.contains("iVal_idx"));
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testSizeClear() throws Exception {
        IgniteEx ig = grid(0);

        final IgniteCache<Integer, DbValue> cache = ig.cache(null);

        GridCacheAdapter<Object, Object> internalCache = ig.context().cache().internalCache();

        int cnt = 5000;

        X.println("Put start");

        for (int i = 0; i < cnt; i++) {
            DbValue v0 = new DbValue(i, "test-value", i);

            cache.put(i, v0);

            checkEmpty(internalCache, i);

            assertEquals(v0, cache.get(i));
        }

        awaitPartitionMapExchange();

        assertEquals(cnt, cache.size(CachePeekMode.OFFHEAP));

        X.println("Clear start.");

        cache.clear();

        assertEquals(0, cache.size(CachePeekMode.OFFHEAP));

        for (int i = 0; i < cnt; i++)
            assertNull(cache.get(i));
    }
    /**
     * @throws Exception if failed.
     */
    public void testBounds() throws Exception {
        IgniteEx ig = grid(0);

        final IgniteCache<Integer, DbValue> cache = ig.cache(null);

        X.println("Put start");

        int cnt = 1000;

        try (IgniteDataStreamer<Integer, DbValue> st = ig.dataStreamer(null)) {
            st.allowOverwrite(true);

            for (int i = 0; i < cnt; i++) {
                int k = 2 * i;

                DbValue v0 = new DbValue(k, "test-value", k);

                st.addData(k, v0);
            }
        }

        X.println("Get start");

        for (int i = 0; i < cnt; i++) {
            int k = 2 * i;

            DbValue v0 = new DbValue(k, "test-value", k);

            assertEquals(v0, cache.get(k));
        }

        if (indexingEnabled()) {
            awaitPartitionMapExchange();

            X.println("Query start");

            // Make sure to cover multiple pages.
            int limit = 500;

            for (int i = 0; i < limit; i++) {
                List<List<?>> res = cache.query(new SqlFieldsQuery("select ival, _val from dbvalue where ival < ? order by ival")
                    .setArgs(i)).getAll();

                // 0 => 0, 1 => 1, 2=>1,...
                assertEquals((i + 1) / 2, res.size());

                res = cache.query(new SqlFieldsQuery("select ival, _val from dbvalue where ival <= ? order by ival")
                    .setArgs(i)).getAll();

                // 0 => 1, 1 => 1, 2=>2,...
                assertEquals(i / 2 + 1, res.size());
            }
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testMultithreadedPut() throws Exception {
        IgniteEx ig = grid(0);

        final IgniteCache<Integer, DbValue> cache = ig.cache(null);

        X.println("Put start");

        int cnt = 20_000;

        try (IgniteDataStreamer<Integer, DbValue> st = ig.dataStreamer(null)) {
            st.allowOverwrite(true);

            for (int i = 0; i < cnt; i++) {
                DbValue v0 = new DbValue(i, "test-value", i);

                st.addData(i, v0);
            }
        }

        X.println("Get start");

        for (int i = 0; i < cnt; i++) {
            DbValue v0 = new DbValue(i, "test-value", i);

            assertEquals(v0, cache.get(i));
        }

        if (indexingEnabled()) {
            awaitPartitionMapExchange();

            X.println("Query start");

            assertEquals(cnt, cache.query(new SqlFieldsQuery("select null from dbvalue")).getAll().size());

            int limit = 500;

            List<List<?>> res = cache.query(new SqlFieldsQuery("select ival, _val from dbvalue where ival < ? order by ival")
                .setArgs(limit)).getAll();

            assertEquals(limit, res.size());

            for (int i = 0; i < limit; i++) {
                List<?> row = res.get(i);

                assertEquals(2, row.size());
                assertEquals(i, row.get(0));

                assertEquals(new DbValue(i, "test-value", i), row.get(1));
            }

            assertEquals(1, cache.query(new SqlFieldsQuery("select lval from dbvalue where ival = 7899")).getAll().size());
            assertEquals(2000, cache.query(new SqlFieldsQuery("select lval from dbvalue where ival >= 5000 and ival < 7000"))
                .getAll().size());

            String plan = cache.query(new SqlFieldsQuery(
                "explain select lval from dbvalue where ival >= 5000 and ival < 7000")).getAll().get(0).get(0).toString();

            assertTrue(plan, plan.contains("iVal_idx"));
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testPutGetRandomUniqueMultipleObjects() throws Exception {
        IgniteEx ig = grid(0);

        final IgniteCache<Integer, DbValue> cache = ig.cache(null);

        GridCacheAdapter<Object, Object> internalCache = ig.context().cache().internalCache();

        int cnt = 100_000;

        Random rnd = new GridRandom();

        int[] keys = generateUniqueRandomKeys(cnt, rnd);

        X.println("Put start");

        for (int i : keys) {
            DbValue v0 = new DbValue(i, "test-value", i);

//            if (i % 100 == 0)
//                X.println(" --> " + i);

            cache.put(i, v0);

            checkEmpty(internalCache, i);

            assertEquals(v0, cache.get(i));
//            for (int j : keys) {
//                if (j == i)
//                    break;
//
//                assertEquals( i + ", " + j, new DbValue(j, "test-value", j), cache.get(j));
//            }
        }

        X.println("Get start");

        for (int i = 0; i < cnt; i++) {
            DbValue v0 = new DbValue(i, "test-value", i);

            checkEmpty(internalCache, i);

//            X.println(" <-- " + i);

            assertEquals(v0, cache.get(i));
        }
    }

    /** */
    private static int[] generateUniqueRandomKeys(int cnt, Random rnd) {
        int[] keys = new int[cnt];

        for (int i = 0; i < cnt; i++)
            keys[i] = i;

        for (int i = 0; i < cnt; i++) {
            int a = rnd.nextInt(cnt);
            int b = rnd.nextInt(cnt);

            int k = keys[a];
            keys[a] = keys[b];
            keys[b] = k;
        }

        return keys;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutPrimaryUniqueSecondaryDuplicates() throws Exception {
        IgniteEx ig = grid(0);

        final IgniteCache<Integer, DbValue> cache = ig.cache(null);

        GridCacheAdapter<Object, Object> internalCache = ig.context().cache().internalCache();

        int cnt = 100_000;

        Random rnd = new GridRandom();

        Map<Integer, DbValue> map = new HashMap<>();

        int[] keys = generateUniqueRandomKeys(cnt, rnd);

        X.println("Put start");

        for (int i : keys) {
            DbValue v0 = new DbValue(rnd.nextInt(30), "test-value", i);

//            X.println(" --> " + i);

            cache.put(i, v0);
            map.put(i, v0);

            checkEmpty(internalCache, i);

            assertEquals(v0, cache.get(i));
        }

        X.println("Get start");

        for (int i = 0; i < cnt; i++) {
            DbValue v0 = map.get(i);

            checkEmpty(internalCache, i);

//            X.println(" <-- " + i);

            assertEquals(v0, cache.get(i));
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testPutGetRandomNonUniqueMultipleObjects() throws Exception {
        IgniteEx ig = grid(0);

        final IgniteCache<Integer, DbValue> cache = ig.cache(null);

        GridCacheAdapter<Object, Object> internalCache = ig.context().cache().internalCache();

        int cnt = 100_000;

        Random rnd = new GridRandom();

        Map<Integer, DbValue> map = new HashMap<>();

        X.println("Put start");

        for (int a = 0; a < cnt; a++) {
            int i = rnd.nextInt();
            int k = rnd.nextInt(cnt);

            DbValue v0 = new DbValue(k, "test-value", i);

//            if (a % 100 == 0)
//                X.println(" --> " + k + " = " + i);

            map.put(k, v0);
            cache.put(k, v0);

            checkEmpty(internalCache, k);

            assertEquals(v0, cache.get(k));
//            for (Map.Entry<Integer,DbValue> entry : map.entrySet())
//                assertEquals(entry.getValue(), cache.get(entry.getKey()));
        }

        X.println("Get start: " + map.size());

        for (int i : map.keySet()) {
            checkEmpty(internalCache, i);

//            X.println(" <-- " + i);

            assertEquals(map.get(i), cache.get(i));
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testPutGetRemoveMultipleForward() throws Exception {
        IgniteEx ig = grid(0);

        final IgniteCache<Integer, DbValue> cache = ig.cache(null);

        GridCacheAdapter<Object, Object> internalCache = ig.context().cache().internalCache();

        int cnt = 100_000;

        X.println("Put.");

        for (int i = 0; i < cnt; i++) {
            DbValue v0 = new DbValue(i, "test-value", i);

//            if (i % 100 == 0)
//                X.println(" --> " + i);

            cache.put(i, v0);

            checkEmpty(internalCache, i);

            assertEquals(v0, cache.get(i));
        }

        X.println("Start removing.");

        for (int i = 0; i < cnt; i++) {
            if (i % 1000 == 0) {
                X.println("-> " + i);

//                assertEquals((long)(cnt - i),
//                    cache.query(new SqlFieldsQuery("select count(*) from dbvalue")).getAll().get(0).get(0));
            }

            cache.remove(i);

            assertNull(cache.get(i));

            if (i + 1 < cnt)
                assertEquals(new DbValue(i + 1, "test-value", i + 1), cache.get(i + 1));
        }
    }

    public void _testRandomPutGetRemove() {
        IgniteEx ig = grid(0);

        final IgniteCache<Integer, DbValue> cache = ig.cache(null);

        int cnt = 100_000;

        Map<Integer, DbValue> map = new HashMap<>(cnt);

        long seed = 1460943282308L; // System.currentTimeMillis();

        X.println(" seed---> " + seed);

        Random rnd = new GridRandom(seed);

        for (int i = 0 ; i < 1000_000; i++) {
            if (i % 5000 == 0)
                X.println(" --> " + i);

            int key = rnd.nextInt(cnt);

            DbValue v0 = new DbValue(key, "test-value-" + rnd.nextInt(200), rnd.nextInt(500));

            switch (rnd.nextInt(3)) {
                case 0:
                    X.println("Put: " + key + " = " + v0);

                    assertEquals(map.put(key, v0), cache.getAndPut(key, v0));

                case 1:
                    X.println("Get: " + key);

                    assertEquals(map.get(key), cache.get(key));

                    break;

                case 2:
                    X.println("Rmv: " + key);

                    assertEquals(map.remove(key), cache.getAndRemove(key));

                    assertNull(cache.get(key));
            }
        }

//        assertEquals(map.size(), cache.size(CachePeekMode.ALL));

        for (Integer key : map.keySet())
            assertEquals(map.get(key), cache.get(key));
    }

    public void testPutGetRemoveMultipleBackward() throws Exception {
        IgniteEx ig = grid(0);

        final IgniteCache<Integer, DbValue> cache = ig.cache(null);

        GridCacheAdapter<Object, Object> internalCache = ig.context().cache().internalCache();

        int cnt = 100_000;

        X.println("Put.");

        for (int i = 0; i < cnt; i++) {
            DbValue v0 = new DbValue(i, "test-value", i);

//            if (i % 100 == 0)
//                X.println(" --> " + i);

            cache.put(i, v0);

            checkEmpty(internalCache, i);

            assertEquals(v0, cache.get(i));
        }

        X.println("Start removing in backward direction.");

        for (int i = cnt - 1; i >= 0; i--) {
            if (i % 1000 == 0) {
                X.println("-> " + i);

//                assertEquals((long)(cnt - i),
//                    cache.query(new SqlFieldsQuery("select count(*) from dbvalue")).getAll().get(0).get(0));
            }

            cache.remove(i);

            assertNull(cache.get(i));

            if (i - 1 >= 0)
                assertEquals(new DbValue(i - 1, "test-value", i - 1), cache.get(i - 1));
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testIndexOverwrite() throws Exception {
        IgniteEx ig = grid(0);

        final IgniteCache<Integer, DbValue> cache = ig.cache(null);

        GridCacheAdapter<Object, Object> internalCache = ig.context().cache().internalCache("non-primitive");

        X.println("Put start");

        int cnt = 10_000;

        for (int a = 0; a < cnt; a++) {
            DbValue v0 = new DbValue(a, "test-value-" + a, a);

            DbKey k0 = new DbKey(a);

            cache.put(a, v0);

            checkEmpty(internalCache, k0);
        }

        info("Update start");

        for (int k = 0; k < 4000; k++) {
            int batchSize = 20;

            LinkedHashMap<Integer, DbValue> batch = new LinkedHashMap<>();

            for (int i = 0; i < batchSize; i++) {
                int a = ThreadLocalRandom.current().nextInt(cnt);

                DbValue v0 = new DbValue(a, "test-value-" + a, a);

                batch.put(a, v0);
            }

            cache.putAll(batch);

            cache.remove(ThreadLocalRandom.current().nextInt(cnt));
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testObjectKey() throws Exception {
        IgniteEx ig = grid(0);

        final IgniteCache<DbKey, DbValue> cache = ig.cache("non-primitive");

        GridCacheAdapter<Object, Object> internalCache = ig.context().cache().internalCache("non-primitive");

        int cnt = 100_000;

        Map<DbKey, DbValue> map = new HashMap<>();

        X.println("Put start");

        for (int a = 0; a < cnt; a++) {
            DbValue v0 = new DbValue(a, "test-value", a);

//            if (a % 100 == 0)
//                X.println(" --> " + k + " = " + i);

            DbKey k0 = new DbKey(a);

            map.put(k0, v0);
            cache.put(k0, v0);

            checkEmpty(internalCache, k0);

//            assertEquals(v0, cache.get(k0));
//            for (Map.Entry<Integer,DbValue> entry : map.entrySet())
//                assertEquals(entry.getValue(), cache.get(entry.getKey()));
        }

        X.println("Get start: " + map.size());

        for (DbKey i : map.keySet()) {
//            checkEmpty(internalCache, i);

//            X.println(" <-- " + i);

            assertEquals(map.get(i), cache.get(i));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testIterators() throws Exception {
        IgniteEx ignite = grid(0);

        IgniteCache<DbKey, DbValue> cache = ignite.cache("non-primitive");

        Affinity<Object> aff = ignite.affinity(cache.getName());

        Map<UUID, Integer> cntrs = new HashMap<>();
        Map<Integer, Integer> partCntrs = new HashMap<>();

        final int ENTRIES = 10_000;

        for (int i = 0; i < ENTRIES; i++) {
            DbKey k = new DbKey(i);
            DbValue v = new DbValue(i, "test-value", i);

            cache.put(k, v);

            UUID nodeId = aff.mapKeyToNode(k).id();

            Integer cntr = cntrs.get(nodeId);

            if (cntr == null)
                cntr = 1;
            else
                cntr += 1;

            cntrs.put(nodeId, cntr);

            int part = aff.partition(k);

            Integer partCntr = partCntrs.get(part);

            if (partCntr == null)
                partCntr = 1;
            else
                partCntr += 1;

            partCntrs.put(part, partCntr);
        }

        checkLocalEntries(ENTRIES, cntrs);

        checkLocalScan(ENTRIES, cntrs);

        checkScan(ENTRIES);

        checkScanPartition(partCntrs);
    }

    /**
     * @param total Expected total entries.
     * @param cntrs Expected per-node entries count.
     */
    private void checkLocalEntries(int total, Map<UUID, Integer> cntrs) {
        Set<DbKey> allKeys = new HashSet<>();

        for (int i = 0; i < gridCount(); i++) {
            Ignite ignite0 = grid(i);

            IgniteCache<DbKey, DbValue> cache0 = ignite0.cache("non-primitive");

            int cnt = 0;

            for (Cache.Entry<DbKey, DbValue> e : cache0.localEntries()) {
                cnt++;

                allKeys.add(e.getKey());
                assertEquals(e.getKey().val, e.getValue().iVal);
            }

            assertEquals(cntrs.get(ignite0.cluster().localNode().id()), (Integer)cnt);
        }

        assertEquals(total, allKeys.size());
    }

    /**
     * @param total Expected total entries.
     * @param cntrs Expected per-node entries count.
     */
    private void checkLocalScan(int total, Map<UUID, Integer> cntrs) {
        Set<DbKey> allKeys = new HashSet<>();

        for (int i = 0; i < gridCount(); i++) {
            Ignite ignite0 = grid(i);

            IgniteCache<DbKey, DbValue> cache0 = ignite0.cache("non-primitive");

            int cnt = 0;

            ScanQuery<DbKey, DbValue> qry = new ScanQuery<>();

            qry.setLocal(true);

            QueryCursor<Cache.Entry<DbKey, DbValue>> cur = cache0.query(qry);

            Map<Integer, Integer> partCntrs = new HashMap<>();

            Affinity<Object> aff = ignite0.affinity(cache0.getName());

            for (Cache.Entry<DbKey, DbValue> e : cur) {
                cnt++;

                allKeys.add(e.getKey());
                assertEquals(e.getKey().val, e.getValue().iVal);

                int part = aff.partition(e.getKey());

                Integer partCntr = partCntrs.get(part);

                if (partCntr == null)
                    partCntr = 1;
                else
                    partCntr += 1;

                partCntrs.put(part, partCntr);
            }

            assertEquals(cntrs.get(ignite0.cluster().localNode().id()), (Integer)cnt);

            checkScanPartition(ignite0, cache0, partCntrs, true);
        }

        assertEquals(total, allKeys.size());
    }

    /**
     * @param total Expected total entries.
     */
    private void checkScan(int total) {
        for (int i = 0; i < gridCount(); i++) {
            Set<DbKey> allKeys = new HashSet<>();

            Ignite ignite0 = grid(i);

            IgniteCache<DbKey, DbValue> cache0 = ignite0.cache("non-primitive");

            ScanQuery<DbKey, DbValue> qry = new ScanQuery<>();

            QueryCursor<Cache.Entry<DbKey, DbValue>> cur = cache0.query(qry);

            for (Cache.Entry<DbKey, DbValue> e : cur) {
                allKeys.add(e.getKey());
                assertEquals(e.getKey().val, e.getValue().iVal);
            }

            assertEquals(total, allKeys.size());
        }
    }

    /**
     * @param partCntrs Expected per-partition entries count.
     */
    private void checkScanPartition(Map<Integer, Integer> partCntrs) {
        for (int i = 0; i < gridCount(); i++) {
            Ignite ignite0 = grid(i);

            IgniteCache<DbKey, DbValue> cache0 = ignite0.cache("non-primitive");

            checkScanPartition(ignite0, cache0, partCntrs, false);
        }
    }

    /**
     * @param partCntrs Expected per-partition entries count.
     */
    private void checkScanPartition(Ignite ignite,
        IgniteCache<DbKey, DbValue> cache,
        Map<Integer, Integer> partCntrs,
        boolean loc) {
        Affinity<Object> aff = ignite.affinity(cache.getName());

        int parts = aff.partitions();

        for (int p = 0; p < parts; p++) {
            ScanQuery<DbKey, DbValue> qry = new ScanQuery<>();

            qry.setPartition(p);
            qry.setLocal(loc);

            if (loc && !ignite.cluster().localNode().equals(aff.mapPartitionToNode(p)))
                continue;

            QueryCursor<Cache.Entry<DbKey, DbValue>> cur = cache.query(qry);

            Set<DbKey> allKeys = new HashSet<>();

            for (Cache.Entry<DbKey, DbValue> e : cur) {
                allKeys.add(e.getKey());
                assertEquals(e.getKey().val, e.getValue().iVal);
            }

            Integer exp = partCntrs.get(p);

            if (exp == null)
                exp = 0;

            assertEquals(exp, (Integer)allKeys.size());
        }
    }

    private void checkEmpty(final GridCacheAdapter internalCache, final Object key) throws Exception {
        GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return internalCache.peekEx(key) == null;
            }
        }, 5000);

        assertNull(internalCache.peekEx(key));
    }

    /**
     *
     */
    private static class DbKey implements Serializable {
        /** */
        private int val;

        /**
         * @param val Value.
         */
        private DbKey(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || !(o instanceof DbKey))
                return false;

            DbKey key = (DbKey)o;

            return val == key.val;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val;
        }
    }

    /**
     *
     */
    private static class DbValue implements Serializable {
        /** */
        @QuerySqlField(index = true)
        private int iVal;

        /** */
        @QuerySqlField(index = true)
        private String sVal;

        /** */
        @QuerySqlField
        private long lVal;

        /**
         * @param iVal Integer value.
         * @param sVal String value.
         * @param lVal Long value.
         */
        public DbValue(int iVal, String sVal, long lVal) {
            this.iVal = iVal;
            this.sVal = sVal;
            this.lVal = lVal;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            DbValue dbVal = (DbValue)o;

            return iVal == dbVal.iVal && lVal == dbVal.lVal &&
                !(sVal != null ? !sVal.equals(dbVal.sVal) : dbVal.sVal != null);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = iVal;

            res = 31 * res + (sVal != null ? sVal.hashCode() : 0);
            res = 31 * res + (int)(lVal ^ (lVal >>> 32));

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DbValue.class, this);
        }
    }
}
