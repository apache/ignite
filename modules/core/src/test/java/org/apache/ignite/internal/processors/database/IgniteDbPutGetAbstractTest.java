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

import java.util.Arrays;
import java.util.Collections;
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
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.util.GridRandom;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.GridTestUtils.SF;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

/**
 *
 */
public abstract class IgniteDbPutGetAbstractTest extends IgniteDbAbstractTest {
    /** */
    private static final int KEYS_COUNT = SF.applyLB(10_000, 2_000);

    /**
     * @return Ignite instance for testing.
     */
    private IgniteEx ig() {
        if (withClientNearCache())
            return grid(gridCount());

        return grid(0);
    }

    /**
     * @return Cache for testing.
     * @throws Exception If failed.
     */
    private <K, V> IgniteCache<K, V> cache(String name) throws Exception {
        if (withClientNearCache())
            return ig().getOrCreateNearCache(name, new NearCacheConfiguration<K, V>());

        return ig().cache(name);
    }

    /**
     *
     */
    @Test
    public void testGradualRandomPutAllRemoveAll() throws Exception {
        IgniteCache<Integer, DbValue> cache = cache(DEFAULT_CACHE_NAME);

        final int cnt = KEYS_COUNT;

        Random rnd = new Random();

        Map<Integer, DbValue> map = new HashMap<>();

        for (int i = 0; i < SF.applyLB(5, 3); i++) {
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
    @Test
    public void testRandomRemove() throws Exception {
        IgniteCache<Integer, DbValue> cache = cache(DEFAULT_CACHE_NAME);

        final int cnt = SF.apply(30_000);

        long seed = System.nanoTime();

        X.println("Seed: " + seed);

        Random rnd = new GridRandom(seed);

        int[] keys = generateUniqueRandomKeys(cnt, rnd);

        X.println("Put start");

        for (int i : keys) {
            DbValue v0 = new DbValue(i, "test-value", i);

            cache.put(i, v0);

            assertEquals(v0, cache.get(i));
        }

        keys = generateUniqueRandomKeys(cnt, rnd);

        X.println("Rmv start");

        for (int i : keys)
            assertTrue(cache.remove(i));
    }

    /**
     */
    @Test
    public void testRandomPut() throws Exception {
        IgniteCache<Integer, DbValue> cache = cache(DEFAULT_CACHE_NAME);

        final int cnt = 1_000;

        long seed = System.nanoTime();

        X.println("Seed: " + seed);

        Random rnd = new GridRandom(seed);

        for (int i = 0; i < 50_000; i++) {
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
    @Test
    public void testPutGetSimple() throws Exception {
        IgniteCache<Integer, DbValue> cache = cache(DEFAULT_CACHE_NAME);

        GridCacheAdapter<Integer, DbValue> internalCache = internalCache(cache);

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
    @Test
    public void testPutGetLarge() throws Exception {
        IgniteCache<Integer, byte[]> cache = cache(DEFAULT_CACHE_NAME);

        final byte[] val = new byte[2048];

        ThreadLocalRandom.current().nextBytes(val);

        cache.put(0, val);

        Assert.assertArrayEquals(val, cache.get(0));

        final IgniteCache<Integer, LargeDbValue> cache1 = cache("large");

        final LargeDbValue large = new LargeDbValue("str1", "str2", randomInts(1024));

        cache1.put(1, large);

        assertEquals(large, cache1.get(1));

        if (indexingEnabled()) {
            final List<Cache.Entry<Integer, LargeDbValue>> all = cache1.query(
                new SqlQuery<Integer, LargeDbValue>(LargeDbValue.class, "str1='str1'")).getAll();

            assertEquals(1, all.size());

            final Cache.Entry<Integer, LargeDbValue> entry = all.get(0);

            assertEquals(1, entry.getKey().intValue());

            assertEquals(large, entry.getValue());
        }

        cache.remove(0);
        cache1.remove(1);

        assertNull(cache.get(0));
        assertNull(cache1.get(1));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutGetLargeKeys() throws Exception {
        IgniteCache<LargeDbKey, Integer> cache = ignite(0).cache(DEFAULT_CACHE_NAME);

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        Map<Integer, LargeDbKey> keys = new HashMap<>();

        for (int i = 0; i < 100; i++) {
            LargeDbKey key = new LargeDbKey(i, 512 + rnd.nextInt(1024));

            assertNull(cache.get(key));

            cache.put(key, i);

            keys.put(i, key);
        }

        Map<LargeDbKey, Integer> res = cache.getAll(new HashSet<>(keys.values()));

        assertEquals(keys.size(), res.size());

        for (Map.Entry<Integer, LargeDbKey> e : keys.entrySet())
            assertEquals(e.getKey(), res.get(e.getValue()));

        cache.removeAll(new HashSet<>(keys.values()));

        for (LargeDbKey key : keys.values())
            assertNull(cache.get(key));
    }

    /**
     * @param size Array size.
     * @return Array with random items.
     */
    private int[] randomInts(final int size) {
        final ThreadLocalRandom rnd = ThreadLocalRandom.current();

        final int[] arr = new int[size];

        for (int i = 0; i < arr.length; i++)
            arr[i] = rnd.nextInt();

        return arr;
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testPutGetOverwrite() throws Exception {
        final IgniteCache<Integer, DbValue> cache = cache(DEFAULT_CACHE_NAME);

        GridCacheAdapter<Integer, DbValue> internalCache = internalCache(cache);

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
    @Test
    public void testOverwriteNormalSizeAfterSmallerSize() throws Exception {
        final IgniteCache<Integer, DbValue> cache = cache(DEFAULT_CACHE_NAME);

        GridCacheAdapter<Integer, DbValue> internalCache = internalCache(cache);

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
    @Test
    public void testPutDoesNotTriggerRead() throws Exception {
        IgniteEx ig = grid(0);

        final IgniteCache<Integer, DbValue> cache = ig.cache(DEFAULT_CACHE_NAME);

        cache.put(0, new DbValue(0, "test-value-0", 0));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testPutGetMultipleObjects() throws Exception {
        final IgniteCache<Integer, DbValue> cache = cache(DEFAULT_CACHE_NAME);

        GridCacheAdapter<Integer, DbValue> internalCache = internalCache(cache);

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

        assertEquals(cnt, cache.size());

        if (indexingEnabled()) {
            awaitPartitionMapExchange();

            X.println("Query start");

            assertEquals(cnt, cache.query(new SqlFieldsQuery("select null from dbvalue")).getAll().size());

            List<List<?>> res = cache.query(new SqlFieldsQuery("select ival, _val from dbvalue where ival < ? order by ival asc")
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

            assertTrue(plan, plan.contains("IVAL_IDX"));
        }

        assertTrue(cache.localSize(CachePeekMode.BACKUP) >= 0);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testSizeClear() throws Exception {
        Assume.assumeFalse("https://issues.apache.org/jira/browse/IGNITE-7952", MvccFeatureChecker.forcedMvcc());

        final IgniteCache<Integer, DbValue> cache = cache(DEFAULT_CACHE_NAME);

        GridCacheAdapter<Integer, DbValue> internalCache = internalCache(cache);

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
    @Test
    public void testBounds() throws Exception {
        IgniteEx ig = ig();

        final IgniteCache<Integer, DbValue> cache = cache(DEFAULT_CACHE_NAME);

        X.println("Put start");

        int cnt = 1000;

        try (IgniteDataStreamer<Integer, DbValue> st = ig.dataStreamer(DEFAULT_CACHE_NAME)) {
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
    @Test
    public void testMultithreadedPut() throws Exception {
        IgniteEx ig = ig();

        final IgniteCache<Integer, DbValue> cache = ig.cache(DEFAULT_CACHE_NAME);

        X.println("Put start");

        int cnt = 20_000;

        try (IgniteDataStreamer<Integer, DbValue> st = ig.dataStreamer(DEFAULT_CACHE_NAME)) {
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

            assertTrue(plan, plan.contains("IVAL_IDX"));
        }
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testPutGetRandomUniqueMultipleObjects() throws Exception {
        final IgniteCache<Integer, DbValue> cache = cache(DEFAULT_CACHE_NAME);

        GridCacheAdapter<Integer, DbValue> internalCache = internalCache(cache);

        int cnt = KEYS_COUNT;

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

        Collections.shuffle(Arrays.asList(keys));

        return keys;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutPrimaryUniqueSecondaryDuplicates() throws Exception {
        final IgniteCache<Integer, DbValue> cache = cache(DEFAULT_CACHE_NAME);

        GridCacheAdapter<Integer, DbValue> internalCache = internalCache(cache);

        int cnt = KEYS_COUNT;

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
    @Test
    public void testPutGetRandomNonUniqueMultipleObjects() throws Exception {
        final IgniteCache<Integer, DbValue> cache = cache(DEFAULT_CACHE_NAME);

        GridCacheAdapter<Integer, DbValue> internalCache = internalCache(cache);

        int cnt = KEYS_COUNT;

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
    @Test
    public void testPutGetRemoveMultipleForward() throws Exception {
        final IgniteCache<Integer, DbValue> cache = cache(DEFAULT_CACHE_NAME);

        GridCacheAdapter<Integer, DbValue> internalCache = internalCache(cache);

        int cnt = KEYS_COUNT;

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

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRandomPutGetRemove() throws Exception {
        final IgniteCache<Integer, DbValue> cache = cache(DEFAULT_CACHE_NAME);

        int cnt = KEYS_COUNT;

        Map<Integer, DbValue> map = new HashMap<>(cnt);

        long seed = System.currentTimeMillis();

        int iterations = SF.apply(MvccFeatureChecker.forcedMvcc() ? 30_000 : 90_000);

        X.println("Seed: " + seed);

        X.println("Iterations total: " + iterations);

        Random rnd = new GridRandom(seed);

        for (int i = 0; i < iterations; i++) {
            if (i % 5000 == 0)
                X.println("Iteration #" + i);

            int key = rnd.nextInt(cnt);

            DbValue v0 = new DbValue(key, "test-value-" + rnd.nextInt(200), rnd.nextInt(500));

            switch (rnd.nextInt(3)) {
                case 0:
                    assertEquals(map.put(key, v0), cache.getAndPut(key, v0));

                case 1:
                    assertEquals(map.get(key), cache.get(key));

                    break;

                case 2:
                    assertEquals(map.remove(key), cache.getAndRemove(key));

                    assertNull(cache.get(key));
            }
        }

        assertEquals(map.size(), cache.size());

        for (Cache.Entry<Integer, DbValue> entry : cache.query(new ScanQuery<Integer, DbValue>()))
            assertEquals(map.get(entry.getKey()), entry.getValue());
    }

    @Test
    public void testPutGetRemoveMultipleBackward() throws Exception {
        final IgniteCache<Integer, DbValue> cache = cache(DEFAULT_CACHE_NAME);

        GridCacheAdapter<Integer, DbValue> internalCache = internalCache(cache);

        int cnt = KEYS_COUNT;

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
    @Test
    public void testIndexOverwrite() throws Exception {
        final IgniteCache<Integer, DbValue> cache = cache(DEFAULT_CACHE_NAME);

        GridCacheAdapter<Integer, DbValue> internalCache = internalCache(cache);

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
    @Test
    public void testObjectKey() throws Exception {
        IgniteEx ig = ig();

        final IgniteCache<DbKey, DbValue> cache = cache("non-primitive");

        GridCacheAdapter<DbKey, DbValue> internalCache = internalCache(cache);

        int cnt = KEYS_COUNT;

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
    @Test
    public void testIterators() throws Exception {
        IgniteEx ignite = ig();

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
        if (internalCache.isNear()) {
            checkEmpty(((GridNearCacheAdapter)internalCache).dht(), key);

            return;
        }

        GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return internalCache.peekEx(key) == null;
            }
        }, 5000);

        assertNull(internalCache.peekEx(key));
    }
}
