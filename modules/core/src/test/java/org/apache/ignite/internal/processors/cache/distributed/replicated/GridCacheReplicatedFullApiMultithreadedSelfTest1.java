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

package org.apache.ignite.internal.processors.cache.distributed.replicated;

import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;

/**
 * TODO: GG-4036: include it to cache suite once it is fixed properly.
 * TODO: Also it is necessary to restore compilation after migrating to open-source.
 */
public class GridCacheReplicatedFullApiMultithreadedSelfTest1 extends GridCacheAbstractSelfTest {
//    /** Mutex. */
//    private static final Object mux = new Object();
//
    /** Grid count. */
    private static final int GRID_CNT = 2;
//
//    /** Thread count. */
//    private static final int THREAD_CNT = 2;
//
//    /** Wait timeout. */
//    private static final long WAIT_TIMEOUT = 5000;
//
//    /** Thread name */
//    private static final String THREAD_NAME = "grid.cache.test.thread";
//
//    /** Key. */
//    private static final String KEY = "key";
//
//    /** Registered event listeners. */
//    private static final GridTestEventListener[] lsnrs = new GridTestEventListener[GRID_CNT];
//
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        assert GRID_CNT > 0;

        return GRID_CNT;
    }
//
//    /** {@inheritDoc} */
//    private int getThreadCount() {
//        assert THREAD_CNT > 0;
//
//        return THREAD_CNT;
//    }
//
//    /** {@inheritDoc} */
//    @Override protected void beforeTestsStarted() throws Exception {
//        super.beforeTestsStarted();
//
//        int cnt = gridCount();
//
//        for (int i = 0; i < cnt; i++) {
//            GridTestEventListener lsnr = new GridTestEventListener(i);
//
//            grid(i).events().localListen(lsnr, EVTS_CACHE);
//
//            lsnrs[i] = lsnr;
//        }
//    }
//
//    /**
//     * @throws Exception If test failed.
//     */
//    public void testA() throws Exception {
//        runMultiThreadedTest(new GridTestCacheRunnable() {
//            @Override public void run(Cache<String, Integer> cache, int idx) throws Exception {
//                int size = 8;
//
//                Map<String, Integer> pairs = threadPairs(size, idx);
//
//                for (Map.Entry<String, Integer> pair : pairs.entrySet()) {
//                    assert !cache.containsKey(pair.getKey());
//                    assert !cache.containsValue(pair.getValue());
//                }
//
//                String[] keys = pairs.keySet().toArray(new String[size]);
//
//                String key = keys[0];
//                int val = pairs.get(key);
//
//                assert cache.put(key, val) == null;
//                assert cache.put(key, val) == val;
//
//                assert cache.get(key) == val;
//
//                key = keys[1];
//                val = pairs.get(key);
//
//                assert cache.putAsync(key, val).get() == null;
//                assert cache.putAsync(key, val).get() == val;
//
//                assert cache.getAsync(key).get() == val;
//
//                key = keys[2];
//                val = pairs.get(key);
//
//                assert cache.putx(key, val);
//                assert cache.putx(key, val);
//
//                assert cache.get(key) == val;
//
//                key = keys[3];
//                val = pairs.get(key);
//
//                assert cache.putxAsync(key, val).get();
//                assert cache.putxAsync(key, val).get();
//
//                assert cache.get(key) == val;
//
//                key = keys[4];
//                val = pairs.get(key);
//
//                assert cache.putIfAbsent(key, val) == null;
//                assert cache.putIfAbsent(key, val + 1) == val;
//
//                assert cache.get(key) == val;
//
//                key = keys[5];
//                val = pairs.get(key);
//
//                assert cache.putIfAbsentAsync(key, val).get() == null;
//                assert cache.putIfAbsentAsync(key, val + 1).get() == val;
//
//                assert cache.get(key) == val;
//
//                key = keys[6];
//                val = pairs.get(key);
//
//                assert cache.putxIfAbsent(key, val);
//                assert !cache.putxIfAbsent(key, val + 1);
//
//                assert cache.get(key) == val;
//
//                key = keys[7];
//                val = pairs.get(key);
//
//                assert cache.putxIfAbsentAsync(key, val).get();
//                assert !cache.putxIfAbsentAsync(key, val + 1).get();
//
//                assert cache.get(key) == val;
//
//                for (Map.Entry<String, Integer> pair : pairs.entrySet()) {
//                    assert cache.containsKey(pair.getKey());
//                    assert cache.containsValue(pair.getValue());
//                }
//            }
//        }, F.t(EVT_CACHE_OBJECT_PUT, 12, false), F.t(EVT_CACHE_OBJECT_READ, 8, true));
//    }
//
//    /**
//     * @throws Exception If test failed.
//     */
//    public void testB() throws Exception {
//        runMultiThreadedTest(new GridTestCacheRunnable() {
//            @Override public void run(Cache<String, Integer> cache, int idx) throws Exception {
//                int size = 8;
//
//                Map<String, Integer> pairs = commonPairs(size);
//
//                String[] keys = pairs.keySet().toArray(new String[size]);
//
//                GridCacheTx tx = cache.txStart();
//
//                for (Map.Entry<String, Integer> pair : pairs.entrySet()) {
//                    assert !cache.containsKey(pair.getKey());
//                    assert !cache.containsValue(pair.getValue());
//                }
//
//                String key = keys[0];
//                int val = pairs.get(key);
//
//                assert cache.put(key, val) == null;
//                assert cache.put(key, val) == val;
//
//                assert cache.get(key) == val;
//
//                key = keys[1];
//                val = pairs.get(key);
//
//                assert cache.putAsync(key, val).get() == null;
//                assert cache.putAsync(key, val).get() == val;
//
//                assert cache.getAsync(key).get() == val;
//
//                key = keys[2];
//                val = pairs.get(key);
//
//                assert cache.putx(key, val);
//                assert cache.putx(key, val);
//
//                assert cache.get(key) == val;
//
//                key = keys[3];
//                val = pairs.get(key);
//
//                assert cache.putxAsync(key, val).get();
//                assert cache.putxAsync(key, val).get();
//
//                assert cache.get(key) == val;
//
//                key = keys[4];
//                val = pairs.get(key);
//
//                assert cache.putIfAbsent(key, val) == null;
//                assert cache.putIfAbsent(key, val + 1) == val;
//
//                assert cache.get(key) == val;
//
//                key = keys[5];
//                val = pairs.get(key);
//
//                assert cache.putIfAbsentAsync(key, val).get() == null;
//                assert cache.putIfAbsentAsync(key, val + 1).get() == val;
//
//                assert cache.get(key) == val;
//
//                key = keys[6];
//                val = pairs.get(key);
//
//                assert cache.putxIfAbsent(key, val);
//                assert !cache.putxIfAbsent(key, val + 1);
//
//                assert cache.get(key) == val;
//
//                key = keys[7];
//                val = pairs.get(key);
//
//                assert cache.putxIfAbsentAsync(key, val).get();
//                assert !cache.putxIfAbsentAsync(key, val + 1).get();
//
//                assert cache.get(key) == val;
//
//                for (Map.Entry<String, Integer> pair : pairs.entrySet()) {
//                    assert cache.containsKey(pair.getKey());
//                    assert cache.containsValue(pair.getValue());
//                }
//
//                tx.commit();
//
//                for (Map.Entry<String, Integer> pair : pairs.entrySet()) {
//                    assert cache.containsKey(pair.getKey());
//                    assert cache.containsValue(pair.getValue());
//                }
//            }
//        }, F.t(EVT_CACHE_OBJECT_PUT, 4, false), F.t(EVT_CACHE_OBJECT_PUT, 4, true));
//    }
//
//    /**
//     * @throws Exception If test failed.
//     */
//    public void testC() throws Exception {
//        runMultiThreadedTest(new GridTestCacheRunnable() {
//            @Override public void run(Cache<String, Integer> cache, int idx) throws Exception {
//                int size = 4;
//
//                Map<String, Integer> pairs = threadPairs(size, idx);
//
//                GridPredicate<Entry<String, Integer>> noPrd = F.cacheNoPeekValue();
//                GridPredicate<Entry<String, Integer>> hasPrd = F.cacheHasPeekValue();
//
//                for (Map.Entry<String, Integer> pair : pairs.entrySet()) {
//                    assert !cache.containsKey(pair.getKey(), hasPrd);
//                    assert !cache.containsValue(pair.getValue(), hasPrd);
//                }
//
//                String[] keys = pairs.keySet().toArray(new String[size]);
//
//                String key = keys[0];
//                int val = pairs.get(key);
//
//                assert cache.put(key, val, noPrd) == null;
//                assert cache.put(key, val, hasPrd) == val;
//
//                assert cache.get(key, noPrd) == null;
//                assert cache.get(key, hasPrd) == val;
//
//                key = keys[1];
//                val = pairs.get(key);
//
//                assert cache.putAsync(key, val, noPrd).get() == null;
//                assert cache.putAsync(key, val, hasPrd).get() == val;
//
//                assert cache.getAsync(key, noPrd).get() == null;
//                assert cache.getAsync(key, hasPrd).get() == val;
//
//                key = keys[2];
//                val = pairs.get(key);
//
//                assert cache.putx(key, val, noPrd);
//                assert cache.putx(key, val, hasPrd);
//
//                assert cache.get(key) == val;
//
//                key = keys[3];
//                val = pairs.get(key);
//
//                assert cache.putxAsync(key, val, noPrd).get();
//                assert cache.putxAsync(key, val, hasPrd).get();
//
//                assert cache.get(key) == val;
//
//                for (Map.Entry<String, Integer> pair : pairs.entrySet()) {
//                    assert cache.containsKey(pair.getKey(), hasPrd);
//                    assert cache.containsValue(pair.getValue(), hasPrd);
//                }
//            }
//        }, F.t(EVT_CACHE_OBJECT_PUT, 8, false), F.t(EVT_CACHE_OBJECT_READ, 6, true));
//    }
//
//    /**
//     * @throws Exception If test failed.
//     */
//    public void testD() throws Exception {
//        runMultiThreadedTest(new GridTestCacheRunnable() {
//            @Override public void run(Cache<String, Integer> cache, int idx) throws Exception {
//                int size = 4;
//
//                Map<String, Integer> pairs = commonPairs(size);
//
//                GridCacheTx tx = cache.txStart();
//
//                GridPredicate<Entry<String, Integer>> noPrd = F.cacheNoPeekValue();
//                GridPredicate<Entry<String, Integer>> hasPrd = F.cacheHasPeekValue();
//
//                for (Map.Entry<String, Integer> pair : pairs.entrySet()) {
//                    assert !cache.containsKey(pair.getKey(), hasPrd);
//                    assert !cache.containsValue(pair.getValue(), hasPrd);
//                }
//
//                String[] arr = pairs.keySet().toArray(new String[size]);
//
//                String key = arr[0];
//                int val = pairs.get(key);
//
//                assert cache.put(key, val, hasPrd) == null;
//                assert cache.put(key, val, noPrd) == null;
//
//                assert cache.get(key, noPrd) == null;
//                assert cache.get(key, hasPrd) == val;
//
//                key = arr[1];
//                val = pairs.get(key);
//
//                assert cache.putAsync(key, val, hasPrd).get() == null;
//                assert cache.putAsync(key, val, noPrd).get() == null;
//
//                assert cache.getAsync(key, noPrd).get() == null;
//                assert cache.getAsync(key, hasPrd).get() == val;
//
//                key = arr[2];
//                val = pairs.get(key);
//
//                assert !cache.putx(key, val, hasPrd);
//                assert cache.putx(key, val, noPrd);
//
//                assert cache.get(key) == val;
//
//                key = arr[3];
//                val = pairs.get(key);
//
//                assert !cache.putxAsync(key, val, hasPrd).get();
//                assert cache.putxAsync(key, val, noPrd).get();
//
//                assert cache.get(key) == val;
//
//                for (Map.Entry<String, Integer> pair : pairs.entrySet()) {
//                    assert cache.containsKey(pair.getKey(), hasPrd);
//                    assert cache.containsValue(pair.getValue(), hasPrd);
//                }
//
//                tx.commit();
//
//                for (Map.Entry<String, Integer> pair : pairs.entrySet()) {
//                    assert cache.containsKey(pair.getKey(), hasPrd);
//                    assert cache.containsValue(pair.getValue(), hasPrd);
//                }
//            }
//        }, F.t(EVT_CACHE_OBJECT_PUT, 4, true));
//    }
//
//    /**
//     * @throws Exception If test failed.
//     */
//    public void testE() throws Exception {
//        runMultiThreadedTest(new GridTestCacheRunnable() {
//            @Override public void run(Cache<String, Integer> cache, int idx) throws Exception {
//                int size = 5;
//
//                final Map<String, Integer> pairs = threadPairs(size, idx);
//
//                P1<Entry<String, Integer>> p = new P1<Entry<String, Integer>>() {
//                    @Override public boolean apply(Entry<String, Integer> e) {
//                        String key = e.getKey();
//
//                        Integer val = pairs.get(key);
//
//                        return val != null && val.equals(e.peek());
//                    }
//                };
//
//                Collection<String> keys = pairs.keySet();
//                Collection<Integer> vals = pairs.values();
//
//                assert !cache.containsAllKeys(keys);
//                assert !cache.containsAnyKeys(keys);
//
//                assert !cache.containsAllValues(vals);
//                assert !cache.containsAnyValues(vals);
//
//                assert !cache.containsAnyEntries(p);
//
//                cache.putAll(pairs);
//
//                Map<String, Integer> map = cache.getAll(keys);
//
//                assert map.keySet().containsAll(keys);
//                assert map.values().containsAll(vals);
//
//                assert cache.containsAllKeys(keys);
//                assert cache.containsAnyKeys(keys);
//
//                assert cache.containsAllValues(vals);
//                assert cache.containsAnyValues(vals);
//
//                assert cache.containsAnyEntries(p);
//
//                cache.removeAll(keys);
//
//                assert !cache.containsAllKeys(keys);
//                assert !cache.containsAllValues(vals);
//
//                cache.putAllAsync(pairs).get();
//
//                map = cache.getAllAsync(keys).get();
//
//                assert map.keySet().containsAll(keys);
//                assert map.values().containsAll(vals);
//
//                assert cache.containsAllKeys(keys);
//                assert cache.containsAllValues(vals);
//
//                cache.removeAllAsync(keys).get();
//
//                assert !cache.containsAllKeys(keys);
//                assert !cache.containsAllValues(vals);
//
//                assert !cache.containsAnyEntries(p);
//            }
//        }, F.t(EVT_CACHE_OBJECT_PUT, 10, false), F.t(EVT_CACHE_OBJECT_READ, 10, true),
//            F.t(EVT_CACHE_OBJECT_REMOVED, 10, false));
//    }
//
//    /**
//     * @throws Exception If test failed.
//     */
//    public void testF() throws Exception {
//        runMultiThreadedTest(new GridTestCacheRunnable() {
//            @Override public void run(Cache<String, Integer> cache, int idx) throws Exception {
//                int size = 5;
//
//                final Map<String, Integer> pairs = commonPairs(size);
//
//                P1<Entry<String, Integer>> p = new P1<Entry<String, Integer>>() {
//                    @Override public boolean apply(Entry<String, Integer> e) {
//                        return pairs.get(e.getKey()).equals(e.peek());
//                    }
//                };
//
//                Collection<String> keys = pairs.keySet();
//                Collection<Integer> vals = pairs.values();
//
//                GridCacheTx tx = cache.txStart();
//
//                assert !cache.containsAllKeys(keys);
//                assert !cache.containsAnyKeys(keys);
//
//                assert !cache.containsAllValues(vals);
//                assert !cache.containsAnyValues(vals);
//
//                assert cache.containsAllEntries(p);
//                assert !cache.containsAnyEntries(p);
//
//                cache.putAll(pairs);
//
//                Map<String, Integer> map = cache.getAll(keys);
//
//                assert map.keySet().containsAll(keys);
//                assert map.values().containsAll(vals);
//
//                assert cache.containsAllKeys(keys);
//                assert cache.containsAnyKeys(keys);
//
//                assert cache.containsAllValues(vals);
//                assert cache.containsAnyValues(vals);
//
//                assert cache.containsAllEntries(p);
//                assert cache.containsAnyEntries(p);
//
//                cache.removeAll(keys);
//
//                assert !cache.containsAllKeys(keys);
//                assert !cache.containsAllValues(vals);
//
//                cache.putAllAsync(pairs).get();
//
//                map = cache.getAllAsync(keys).get();
//
//                assert map.keySet().containsAll(keys);
//                assert map.values().containsAll(vals);
//
//                assert cache.containsAllKeys(keys);
//                assert cache.containsAllValues(vals);
//
//                cache.removeAllAsync(keys).get();
//
//                assert !cache.containsAllKeys(keys);
//                assert !cache.containsAllValues(vals);
//
//                tx.commit();
//
//                assert !cache.containsAllKeys(keys);
//                assert !cache.containsAllValues(vals);
//            }
//        }, F.t(EVT_CACHE_OBJECT_REMOVED, 5, false));
//    }
//
//    /**
//     * @throws Exception If test failed.
//     */
//    public void testG() throws Exception {
//        runMultiThreadedTest(new GridTestCacheRunnable() {
//            @Override public void run(Cache<String, Integer> cache, int idx) throws Exception {
//                int size = 5;
//
//                Map<String, Integer> pairs = threadPairs(size, idx);
//
//                GridPredicate<Entry<String, Integer>> noPrd = F.cacheNoPeekValue();
//                GridPredicate<Entry<String, Integer>> hasPrd = F.cacheHasPeekValue();
//
//                Collection<String> keys = pairs.keySet();
//                Collection<Integer> vals = pairs.values();
//
//                assert !cache.containsAllKeys(keys, hasPrd);
//                assert !cache.containsAnyKeys(keys, hasPrd);
//
//                assert !cache.containsAllValues(vals, hasPrd);
//                assert !cache.containsAnyValues(vals, hasPrd);
//
//                cache.putAll(pairs, hasPrd);
//
//                assert !cache.containsAllKeys(keys);
//
//                cache.putAll(pairs, noPrd);
//
//                assert cache.containsAllKeys(keys);
//
//                Map<String, Integer> map = cache.getAll(keys, noPrd);
//
//                assert !map.keySet().containsAll(keys);
//                assert !map.values().containsAll(vals);
//
//                map = cache.getAll(keys, hasPrd);
//
//                assert map.keySet().containsAll(keys);
//                assert map.values().containsAll(vals);
//
//                assert cache.containsAllKeys(keys, hasPrd);
//                assert cache.containsAnyKeys(keys, hasPrd);
//
//                assert cache.containsAllValues(vals, hasPrd);
//                assert cache.containsAnyValues(vals, hasPrd);
//
//                cache.removeAll(keys, noPrd);
//
//                assert cache.containsAllKeys(keys);
//
//                cache.removeAll(keys, hasPrd);
//
//                assert !cache.containsAllKeys(keys);
//
//                cache.putAllAsync(pairs, hasPrd).get();
//
//                assert !cache.containsAllKeys(keys);
//
//                cache.putAllAsync(pairs, noPrd).get();
//
//                assert cache.containsAllKeys(keys);
//
//                map = cache.getAllAsync(keys, noPrd).get();
//
//                assert !map.keySet().containsAll(keys);
//                assert !map.values().containsAll(vals);
//
//                map = cache.getAllAsync(keys, hasPrd).get();
//
//                assert map.keySet().containsAll(keys);
//                assert map.values().containsAll(vals);
//
//                cache.removeAllAsync(keys, noPrd).get();
//
//                assert cache.containsAllKeys(keys);
//
//                cache.removeAllAsync(keys, hasPrd).get();
//
//                assert !cache.containsAllKeys(keys);
//            }
//        }, F.t(EVT_CACHE_OBJECT_PUT, 10, false), F.t(EVT_CACHE_OBJECT_READ, 20, true),
//            F.t(EVT_CACHE_OBJECT_REMOVED, 10, false));
//    }
//
//    /**
//     * @throws Exception If test failed.
//     */
//    public void testH() throws Exception {
//        // Tests put.
//        runMultiThreadedTest(new GridTestCacheRunnable() {
//            @Override public void run(Cache<String, Integer> cache, int idx) throws Exception {
//                int size = 5;
//
//                Map<String, Integer> pairs = commonPairs(size);
//
//                GridPredicate<Entry<String, Integer>> noPrd = F.cacheNoPeekValue();
//                GridPredicate<Entry<String, Integer>> hasPrd = F.cacheHasPeekValue();
//
//                Collection<String> keys = pairs.keySet();
//                Collection<Integer> vals = pairs.values();
//
//                GridCacheTx tx = cache.txStart();
//
//                assert !cache.containsAllKeys(keys, hasPrd);
//                assert !cache.containsAnyKeys(keys, hasPrd);
//
//                assert !cache.containsAllValues(vals, hasPrd);
//                assert !cache.containsAnyValues(vals, hasPrd);
//
//                cache.putAll(pairs, hasPrd);
//
//                assert !cache.containsAllKeys(keys);
//
//                cache.putAll(pairs, noPrd);
//
//                assert cache.containsAllKeys(keys);
//
//                Map<String, Integer> map = cache.getAll(keys, noPrd);
//
//                assert !map.keySet().containsAll(keys);
//                assert !map.values().containsAll(vals);
//
//                map = cache.getAll(keys, hasPrd);
//
//                assert map.keySet().containsAll(keys);
//                assert map.values().containsAll(vals);
//
//                assert cache.containsAllKeys(keys, hasPrd);
//                assert cache.containsAnyKeys(keys, hasPrd);
//
//                assert cache.containsAllValues(vals, hasPrd);
//                assert cache.containsAnyValues(vals, hasPrd);
//
//                cache.removeAll(keys, noPrd);
//
//                assert cache.containsAllKeys(keys);
//
//                cache.removeAll(keys, hasPrd);
//
//                assert !cache.containsAllKeys(keys);
//
//                cache.putAllAsync(pairs, hasPrd).get();
//
//                assert !cache.containsAllKeys(keys);
//
//                cache.putAllAsync(pairs, noPrd).get();
//
//                assert cache.containsAllKeys(keys);
//
//                map = cache.getAllAsync(keys, noPrd).get();
//
//                assert !map.keySet().containsAll(keys);
//                assert !map.values().containsAll(vals);
//
//                map = cache.getAllAsync(keys, hasPrd).get();
//
//                assert map.keySet().containsAll(keys);
//                assert map.values().containsAll(vals);
//
//                cache.removeAllAsync(keys, noPrd).get();
//
//                assert cache.containsAllKeys(keys);
//
//                cache.removeAllAsync(keys, hasPrd).get();
//
//                assert !cache.containsAllKeys(keys);
//
//                tx.commit();
//
//                assert !cache.containsAllKeys(keys);
//                assert !cache.containsAllValues(vals);
//            }
//        }, F.t(EVT_CACHE_OBJECT_REMOVED, 5, false));
//    }
//
//    /**
//     * @throws Exception If test failed.
//     */
//    public void testI() throws Exception {
//        runMultiThreadedTest(new GridTestCacheRunnable() {
//            @Override public void run(Cache<String, Integer> cache, int idx) throws Exception {
//                int size = 6;
//
//                Map<String, Integer> pairs = threadPairs(size, idx);
//
//                String[] keys = pairs.keySet().toArray(new String[size]);
//
//                assert !cache.containsAllKeys(keys);
//
//                cache.putAll(pairs);
//
//                assert cache.containsAllKeys(keys);
//
//                String key = keys[0];
//                int val = pairs.get(key);
//
//                assert cache.remove(key) == val;
//
//                assert !cache.containsKey(key);
//
//                key = keys[1];
//                val = pairs.get(key);
//
//                assert !cache.remove(key, -1);
//                assert cache.remove(key, val);
//
//                assert !cache.containsKey(key);
//
//                key = keys[2];
//
//                assert cache.removex(key);
//
//                assert !cache.containsKey(key);
//
//                key = keys[3];
//                val = pairs.get(key);
//
//                assert cache.removeAsync(key).get() == val;
//
//                assert !cache.containsKey(key);
//
//                key = keys[4];
//                val = pairs.get(key);
//
//                assert !cache.removeAsync(key, -1).get();
//                assert cache.removeAsync(key, val).get();
//
//                assert !cache.containsKey(key);
//
//                key = keys[5];
//
//                assert cache.removexAsync(key).get();
//
//                assert !cache.containsKey(key);
//            }
//        }, F.t(EVT_CACHE_OBJECT_PUT, 6, false), F.t(EVT_CACHE_OBJECT_REMOVED, 6, false));
//    }
//
//    /**
//     * @throws Exception If test failed.
//     */
//    public void testJ() throws Exception {
//        runMultiThreadedTest(new GridTestCacheRunnable() {
//            @Override public void run(Cache<String, Integer> cache, int idx) throws Exception {
//                int size = 6;
//
//                Map<String, Integer> pairs = commonPairs(size);
//
//                String[] keys = pairs.keySet().toArray(new String[size]);
//
//                GridCacheTx tx = cache.txStart();
//
//                assert !cache.containsAllKeys(keys);
//
//                cache.putAll(pairs);
//
//                assert cache.containsAllKeys(keys);
//
//                String key = keys[0];
//                int val = pairs.get(key);
//
//                assert cache.remove(key) == val;
//
//                assert !cache.containsKey(key);
//
//                key = keys[1];
//                val = pairs.get(key);
//
//                assert !cache.remove(key, -1);
//                assert cache.remove(key, val);
//
//                assert !cache.containsKey(key);
//
//                key = keys[2];
//
//                assert cache.removex(key);
//
//                assert !cache.containsKey(key);
//
//                key = keys[3];
//                val = pairs.get(key);
//
//                assert cache.removeAsync(key).get() == val;
//
//                assert !cache.containsKey(key);
//
//                key = keys[4];
//                val = pairs.get(key);
//
//                assert !cache.removeAsync(key, -1).get();
//                assert cache.removeAsync(key, val).get();
//
//                assert !cache.containsKey(key);
//
//                key = keys[5];
//
//                assert cache.removexAsync(key).get();
//
//                assert !cache.containsKey(key);
//
//                tx.commit();
//
//                assert !cache.containsAllKeys(keys);
//            }
//        }, F.t(EVT_CACHE_OBJECT_REMOVED, 6, false));
//    }
//
//    /**
//     * @throws Exception If test failed.
//     */
//    public void testK() throws Exception {
//        runMultiThreadedTest(new GridTestCacheRunnable() {
//            @Override public void run(Cache<String, Integer> cache, int idx) throws Exception {
//                int size = 6;
//
//                Map<String, Integer> pairs = threadPairs(size, idx);
//
//                String[] keys = pairs.keySet().toArray(new String[size]);
//
//                assert !cache.containsAnyKeys(keys);
//                assert !cache.containsAnyValues(pairs.values());
//
//                String key = keys[0];
//                int val = pairs.get(key);
//
//                cache.put(key, val - 1);
//
//                assert cache.replace(key, val) == val - 1;
//
//                assert cache.get(key) == val;
//
//                key = keys[1];
//                val = pairs.get(key);
//
//                cache.put(key, val - 1);
//
//                assert cache.replace(key, val - 1, val);
//                assert !cache.replace(key, val - 1, val);
//
//                assert cache.get(key) == val;
//
//                key = keys[2];
//                val = pairs.get(key);
//
//                cache.put(key, val - 1);
//
//                assert cache.replacex(key, val);
//
//                assert cache.get(key) == val;
//
//                key = keys[3];
//                val = pairs.get(key);
//
//                cache.put(key, val - 1);
//
//                assert cache.replaceAsync(key, val).get() == val - 1;
//
//                assert cache.get(key) == val;
//
//                key = keys[4];
//                val = pairs.get(key);
//
//                cache.put(key, val - 1);
//
//                assert cache.replaceAsync(key, val - 1, val).get();
//                assert !cache.replaceAsync(key, val - 1, val).get();
//
//                assert cache.get(key) == val;
//
//                key = keys[5];
//                val = pairs.get(key);
//
//                cache.put(key, val - 1);
//
//                assert cache.replacexAsync(key, val).get();
//
//                assert cache.get(key) == val;
//
//                assert cache.containsAllKeys(keys);
//                assert cache.containsAllValues(pairs.values());
//            }
//        }, F.t(EVT_CACHE_OBJECT_PUT, 12, false), F.t(EVT_CACHE_OBJECT_READ, 6, true));
//    }
//
//    /**
//     * @throws Exception If test failed.
//     */
//    public void testL() throws Exception {
//        runMultiThreadedTest(new GridTestCacheRunnable() {
//            @Override public void run(Cache<String, Integer> cache, int idx) throws Exception {
//                int size = 6;
//
//                Map<String, Integer> pairs = commonPairs(size);
//
//                String[] keys = pairs.keySet().toArray(new String[size]);
//
//                GridCacheTx tx = cache.txStart();
//
//                assert !cache.containsAnyKeys(keys);
//                assert !cache.containsAnyValues(pairs.values());
//
//                String key = keys[0];
//                int val = pairs.get(key);
//
//                cache.put(key, val - 1);
//
//                assert cache.replace(key, val) == val - 1;
//
//                assert cache.get(key) == val;
//
//                key = keys[1];
//                val = pairs.get(key);
//
//                cache.put(key, val - 1);
//
//                assert cache.replace(key, val - 1, val);
//                assert !cache.replace(key, val - 1, val);
//
//                assert cache.get(key) == val;
//
//                key = keys[2];
//                val = pairs.get(key);
//
//                cache.put(key, val - 1);
//
//                assert cache.replacex(key, val);
//
//                assert cache.get(key) == val;
//
//                key = keys[3];
//                val = pairs.get(key);
//
//                cache.put(key, val - 1);
//
//                assert cache.replaceAsync(key, val).get() == val - 1;
//
//                assert cache.get(key) == val;
//
//                key = keys[4];
//                val = pairs.get(key);
//
//                cache.put(key, val - 1);
//
//                assert cache.replaceAsync(key, val - 1, val).get();
//                assert !cache.replaceAsync(key, val - 1, val).get();
//
//                assert cache.get(key) == val;
//
//                key = keys[5];
//                val = pairs.get(key);
//
//                cache.put(key, val - 1);
//
//                assert cache.replacexAsync(key, val).get();
//
//                assert cache.get(key) == val;
//
//                assert cache.containsAllKeys(keys);
//                assert cache.containsAllValues(pairs.values());
//
//                tx.commit();
//            }
//        }, F.t(EVT_CACHE_OBJECT_PUT, 6, true));
//    }
//
//    /**
//     * @throws Exception If test failed.
//     */
//    public void testM() throws Exception {
//        Cache<String, Integer> cache = cache();
//
//        int size = 10;
//
//        Map<String, Integer> pairs = commonPairs(size);
//
//        final Collection<String> keys = pairs.keySet();
//        Collection<Integer> vals = pairs.values();
//
//        final int min = Collections.min(vals);
//
//        final GridPredicate<Entry<String, Integer>> p = new P1<Entry<String, Integer>>() {
//            @Override public boolean apply(Entry<String, Integer> e) {
//                Integer val = e.peek();
//
//                return val != null && val >= min;
//            }
//        };
//
//        assert cache.forAll(p);
//
//        cache.putAll(pairs);
//
//        waitForEventCount(F.t(EVT_CACHE_OBJECT_PUT, size));
//
//        runMultiThreadedTest(new GridTestCacheRunnable() {
//            @Override public void run(Cache<String, Integer> cache, int idx) throws Exception {
//                assert cache.forAll(p);
//                assert cache.forAllAsync(p).get();
//
//                assert cache.forAll(p, keys);
//                assert cache.forAllAsync(p, keys).get();
//
//                String[] arr = {F.rand(keys), F.rand(keys)};
//
//                assert cache.forAll(p, arr);
//                assert cache.forAllAsync(p, arr).get();
//            }
//        });
//    }
//
//    /**
//     * @throws Exception If test failed.
//     */
//    public void testN() throws Exception {
//        runMultiThreadedTest(new GridTestCacheRunnable() {
//            @Override public void run(Cache<String, Integer> cache, int idx) throws Exception {
//                int size = 10;
//
//                Map<String, Integer> pairs = threadPairs(size, idx);
//
//                Collection<String> keys = pairs.keySet();
//                Collection<Integer> vals = pairs.values();
//
//                final int min = Collections.min(vals);
//
//                GridPredicate<Entry<String, Integer>> p = new P1<Entry<String, Integer>>() {
//                    @Override public boolean apply(Entry<String, Integer> e) {
//                        Integer val = e.peek();
//
//                        return val != null && val >= min;
//                    }
//                };
//
//                GridCacheTx tx = cache.txStart();
//
//                assert cache.forAll(p);
//
//                cache.putAll(pairs);
//
//                assert cache.forAll(p);
//                assert cache.forAllAsync(p).get();
//
//                assert cache.forAll(p, keys);
//                assert cache.forAllAsync(p, keys).get();
//
//                String[] arr = {F.rand(keys), F.rand(keys)};
//
//                assert cache.forAll(p, arr);
//                assert cache.forAllAsync(p, arr).get();
//
//                tx.commit();
//            }
//        }, F.t(EVT_CACHE_OBJECT_PUT, 10, false));
//    }
//
//    /**
//     * @throws Exception If test failed.
//     */
//    public void testO() throws Exception {
//        Cache<String, Integer> cache = cache();
//
//        GridCacheClosure c = new GridCacheClosure();
//
//        cache.forEach(c);
//
//        assert c.getCalculatedValue() == Integer.MAX_VALUE;
//
//        int size = 10;
//
//        Map<String, Integer> pairs = commonPairs(size);
//
//        final Collection<String> keys = pairs.keySet();
//
//        final int min = Collections.min(pairs.values());
//
//        cache.putAll(pairs);
//
//        waitForEventCount(F.t(EVT_CACHE_OBJECT_PUT, size));
//
//        runMultiThreadedTest(new GridTestCacheRunnable() {
//            @Override public void run(Cache<String, Integer> cache, int idx) throws Exception {
//                GridCacheClosure c = new GridCacheClosure();
//
//                cache.forEach(c);
//
//                assert c.getCalculatedValue() == min;
//
//                c.reset();
//
//                cache.forEachAsync(c).get();
//
//                assert c.getCalculatedValue() == min;
//
//                c.reset();
//
//                cache.forEach(c, keys);
//
//                assert c.getCalculatedValue() == min;
//
//                c.reset();
//
//                cache.forEachAsync(c, keys).get();
//
//                assert c.getCalculatedValue() == min : c.getCalculatedValue();
//
//                c.reset();
//
//                String[] arr = keys.toArray(new String[keys.size()]);
//
//                cache.forEach(c, arr);
//
//                assert c.getCalculatedValue() == min : c.getCalculatedValue();
//
//                c.reset();
//
//                cache.forEachAsync(c, arr).get();
//
//                assert c.getCalculatedValue() == min : c.getCalculatedValue();
//            }
//        });
//    }
//
//    /**
//     * @throws Exception If test failed.
//     */
//    public void testP() throws Exception {
//        runMultiThreadedTest(new GridTestCacheRunnable() {
//            @Override public void run(Cache<String, Integer> cache, int idx) throws Exception {
//                int size = 10;
//
//                Map<String, Integer> pairs = threadPairs(size, idx);
//
//                Collection<String> keys = pairs.keySet();
//
//                int min = Collections.min(pairs.values());
//
//                GridCacheTx tx = cache.txStart();
//
//                GridCacheClosure c = new GridCacheClosure();
//
//                cache.forEach(c);
//
//                assert c.getCalculatedValue() == Integer.MAX_VALUE;
//
//                c.reset();
//
//                cache.putAll(pairs);
//
//                cache.forEach(c);
//
//                assert c.getCalculatedValue() == min;
//
//                c.reset();
//
//                cache.forEachAsync(c).get();
//
//                assert c.getCalculatedValue() == min;
//
//                c.reset();
//
//                cache.forEach(c, keys);
//
//                assert c.getCalculatedValue() == min;
//
//                c.reset();
//
//                cache.forEachAsync(c, keys).get();
//
//                assert c.getCalculatedValue() == min : c.getCalculatedValue();
//
//                c.reset();
//
//                String[] arr = keys.toArray(new String[keys.size()]);
//
//                cache.forEach(c, arr);
//
//                assert c.getCalculatedValue() == min : c.getCalculatedValue();
//
//                c.reset();
//
//                cache.forEachAsync(c, arr).get();
//
//                assert c.getCalculatedValue() == min : c.getCalculatedValue();
//
//                tx.commit();
//            }
//        }, F.t(EVT_CACHE_OBJECT_PUT, 10, false));
//    }
//
//    /**
//     * @throws Exception If test failed.
//     */
//    public void testQ() throws Exception {
//        Cache<String, Integer> cache = cache();
//
//        int size = 3;
//
//        Map<String, Integer> pairs = commonPairs(size);
//
//        final String[] keys = pairs.keySet().toArray(new String[size]);
//
//        cache.putAll(pairs);
//
//        waitForEventCount(F.t(EVT_CACHE_OBJECT_PUT, size));
//
//        resetEventCounters();
//
//        final CyclicBarrier b = new CyclicBarrier(gridCount());
//
//        final AtomicInteger lockCnt = new AtomicInteger();
//
//        runMultiThreadedTest(new GridTestCacheRunnable() {
//            @SuppressWarnings({"TooBroadScope"})
//            @Override public void run(Cache<String, Integer> cache, int idx) throws Exception {
//                long timeout = 10;
//
//                String key = keys[0];
//
//                b.await();
//
//                if (cache.lock(key, timeout)) {
//                    try {
//                        assert cache.isLocked(key);
//                        assert cache.isLockedByThread(key);
//
//                        lockCnt.incrementAndGet();
//
//                        b.await();
//                    }
//                    finally {
//                        cache.unlock(key);
//                    }
//                }
//                else {
//                    assert cache.isLocked(key);
//                    assert !cache.isLockedByThread(key);
//
//                    b.await();
//                }
//
//                assert lockCnt.get() == 1;
//
//                b.await();
//
//                if (cache.lockAsync(key, timeout).get()) {
//                    try {
//                        assert cache.isLocked(key);
//                        assert cache.isLockedByThread(key);
//
//                        lockCnt.incrementAndGet();
//
//                        b.await();
//                    }
//                    finally {
//                        cache.unlock(key);
//                    }
//                }
//                else {
//                    assert cache.isLocked(key);
//                    assert !cache.isLockedByThread(key);
//
//                    b.await();
//                }
//
//                assert lockCnt.get() == 2;
//
//                b.await();
//
//                if (cache.lockAll(timeout, keys)) {
//                    try {
//                        for (String key0 : keys) {
//                            assert cache.isLocked(key0);
//                            assert cache.isLockedByThread(key0);
//                        }
//
//                        lockCnt.incrementAndGet();
//
//                        b.await();
//                    }
//                    finally {
//                        cache.unlockAll(keys);
//                    }
//                }
//                else {
//                    for (String key0 : keys) {
//                        assert cache.isLocked(key0);
//                        assert !cache.isLockedByThread(key0);
//                    }
//
//                    b.await();
//                }
//
//                assert lockCnt.get() == 3;
//
//                b.await();
//
//                if (cache.lockAllAsync(timeout, keys).get()) {
//                    try {
//                        for (String key0 : keys) {
//                            assert cache.isLocked(key0);
//                            assert cache.isLockedByThread(key0);
//                        }
//
//                        lockCnt.incrementAndGet();
//
//                        b.await();
//                    }
//                    finally {
//                        cache.unlockAll(keys);
//                    }
//                }
//                else {
//                    for (String key0 : keys) {
//                        assert cache.isLocked(key0);
//                        assert !cache.isLockedByThread(key0);
//                    }
//
//                    b.await();
//                }
//
//                assert lockCnt.get() == 4;
//
//                b.await();
//            }
//        }, F.t(EVT_CACHE_OBJECT_LOCKED, 5, true), F.t(EVT_CACHE_OBJECT_UNLOCKED, 5, true));
//    }
//
//    /**
//     * @throws Exception If test failed.
//     */
//    public void testR() throws Exception {
//        Cache<String, Integer> cache = cache();
//
//        int size = 3;
//
//        Map<String, Integer> pairs = commonPairs(size);
//
//        final String[] keys = pairs.keySet().toArray(new String[size]);
//
//        cache.putAll(pairs);
//
//        waitForEventCount(F.t(EVT_CACHE_OBJECT_PUT, size));
//
//        resetEventCounters();
//
//        final CyclicBarrier b = new CyclicBarrier(gridCount());
//
//        final AtomicInteger lockCnt = new AtomicInteger();
//
//        runMultiThreadedTest(new GridTestCacheRunnable() {
//            @SuppressWarnings({"TooBroadScope"})
//            @Override public void run(Cache<String, Integer> cache, int idx) throws Exception {
//                GridCacheTx tx = cache.txStart();
//
//                long timeout = 10;
//
//                String key = keys[0];
//
//                b.await();
//
//                if (cache.lock(key, timeout)) {
//                    try {
//                        assert cache.isLocked(key);
//                        assert cache.isLockedByThread(key);
//
//                        lockCnt.incrementAndGet();
//
//                        b.await();
//                    }
//                    finally {
//                        cache.unlock(key);
//                    }
//                }
//                else {
//                    assert cache.isLocked(key);
//                    assert !cache.isLockedByThread(key);
//
//                    b.await();
//                }
//
//                assert lockCnt.get() == 1;
//
//                b.await();
//
//                if (cache.lockAsync(key, timeout).get()) {
//                    try {
//                        assert cache.isLocked(key);
//                        assert cache.isLockedByThread(key);
//
//                        lockCnt.incrementAndGet();
//
//                        b.await();
//                    }
//                    finally {
//                        cache.unlock(key);
//                    }
//                }
//                else {
//                    assert cache.isLocked(key);
//                    assert !cache.isLockedByThread(key);
//
//                    b.await();
//                }
//
//                assert lockCnt.get() == 2;
//
//                b.await();
//
//                if (cache.lockAll(timeout, keys)) {
//                    try {
//                        for (String key0 : keys) {
//                            assert cache.isLocked(key0);
//                            assert cache.isLockedByThread(key0);
//                        }
//
//                        lockCnt.incrementAndGet();
//
//                        b.await();
//                    }
//                    finally {
//                        cache.unlockAll(keys);
//                    }
//                }
//                else {
//                    for (String key0 : keys) {
//                        assert cache.isLocked(key0);
//                        assert !cache.isLockedByThread(key0);
//                    }
//
//                    b.await();
//                }
//
//                assert lockCnt.get() == 3;
//
//                b.await();
//
//                if (cache.lockAllAsync(timeout, keys).get()) {
//                    try {
//                        for (String key0 : keys) {
//                            assert cache.isLocked(key0);
//                            assert cache.isLockedByThread(key0);
//                        }
//
//                        lockCnt.incrementAndGet();
//
//                        b.await();
//                    }
//                    finally {
//                        cache.unlockAll(keys);
//                    }
//                }
//                else {
//                    for (String key0 : keys) {
//                        assert cache.isLocked(key0);
//                        assert !cache.isLockedByThread(key0);
//                    }
//
//                    b.await();
//                }
//
//                assert lockCnt.get() == 4;
//
//                b.await();
//
//                tx.commit();
//            }
//        }, F.t(EVT_CACHE_OBJECT_LOCKED, 5, true), F.t(EVT_CACHE_OBJECT_UNLOCKED, 5, true));
//    }
//
//    /**
//     * Runs provided {@link GridTestCacheRunnable} instance on all caches.
//     *
//     * @param idxCacheRunnable {@link GridTestCacheRunnable} instance.
//     * @param evtCnts Array of tuples with values: V1 - event type,
//     *     V2 - expected event count on one node for one thread execution.
//     * @throws Exception In case of error.
//     */
//    private void runMultiThreadedTest(final GridTestCacheRunnable idxCacheRunnable,
//        final GridTuple3<Integer, Integer, Boolean>... evtCnts) throws Exception {
//        final int gridCnt = gridCount();
//        final int threadCnt = getThreadCount();
//
//        final AtomicReference<Exception> err = new AtomicReference<>();
//
//        final AtomicInteger cntr = new AtomicInteger();
//
//        try {
//            GridTestUtils.runMultiThreaded(new Runnable() {
//                @Override public void run() {
//                    try {
//                        int idx = cntr.getAndIncrement();
//
//                        idxCacheRunnable.run(cache(idx % gridCnt), idx);
//                    }
//                    catch (Exception e) {
//                        err.compareAndSet(null, e);
//                    }
//                }
//            }, threadCnt, THREAD_NAME);
//
//            Exception e = err.get();
//
//            if (e != null) {
//                throw e;
//            }
//
//            if (!F.isEmpty(evtCnts)) {
//                cntr.set(0);
//
//                GridTestUtils.runMultiThreaded(new Runnable() {
//                    @Override public void run() {
//                        try {
//                            Map<Integer, GridBiTuple<Integer, Integer>> evtCntMap =
//                                new HashMap<>();
//
//                            int idx = cntr.getAndIncrement();
//
//                            for (GridTuple3<Integer, Integer, Boolean> t3 : evtCnts) {
//                                int cnt = t3.get2() * (!t3.get3() ? threadCnt :
//                                    threadCnt / gridCnt + (threadCnt % gridCnt - 1 >= idx ? 1 : 0));
//
//                                if (cnt > 0) {
//                                    int type = t3.get1();
//
//                                    GridBiTuple<Integer, Integer> t2 = evtCntMap.get(type);
//
//                                    if (t2 == null) {
//                                        evtCntMap.put(type, F.t(type, cnt));
//                                    }
//                                    else {
//                                        t2.set2(t2.get2() + cnt);
//                                    }
//                                }
//                            }
//
//                            lsnrs[idx].waitForEventCount(evtCntMap.values());
//                        }
//                        catch (Exception e) {
//                            err.compareAndSet(null, e);
//                        }
//                    }
//                }, gridCnt, THREAD_NAME);
//
//                e = err.get();
//
//                if (e != null) {
//                    throw e;
//                }
//            }
//
//            checkCaches();
//        }
//        finally {
//            clearCaches();
//
//            resetEventCounters();
//        }
//    }
//
//    /**
//     * Checks caches.
//     */
//    private void checkCaches() {
//        Cache<String, Integer> c0 = cache(0);
//
//        int gridCnt = gridCount();
//
//        for (int i = 1; i < gridCnt; i++) {
//            Cache<String, Integer> ci = cache(i);
//
//            assert ci.size() == c0.size();
//
//            if (!c0.isEmpty()) {
//                assert ci.containsAllKeys(c0.keySet());
//                assert ci.containsAllValues(c0.values());
//            }
//        }
//    }
//
//    /**
//     * Clear caches.
//     */
//    private void clearCaches() {
//        int gridCnt = gridCount();
//
//        for(int i = 0; i < gridCnt; i++) {
//            Cache cache = cache(i);
//
//            cache.clearAll();
//
//            assert cache.isEmpty();
//        }
//    }
//
//    /**
//     * Waits for event count on all nodes.
//     *
//     * @param evtCnts Array of tuples with values: V1 - event type, V2 - expected event count on one node.
//     * @throws InterruptedException If thread has been interrupted while waiting.
//     */
//    private void waitForEventCount(final GridBiTuple<Integer, Integer>... evtCnts) throws Exception {
//        if (!F.isEmpty(evtCnts)) {
//            final AtomicReference<Throwable> err = new AtomicReference<>();
//
//            final AtomicInteger idx = new AtomicInteger();
//
//            GridTestUtils.runMultiThreaded(new Runnable() {
//                @Override public void run() {
//                    try {
//                        lsnrs[idx.getAndIncrement()].waitForEventCount(evtCnts);
//                    }
//                    catch (Throwable t) {
//                        err.compareAndSet(null, t);
//                    }
//                }
//            }, gridCount(), THREAD_NAME);
//
//            Throwable t = err.get();
//
//            if (t instanceof Exception) {
//                throw (Exception)t;
//            }
//            else if (t instanceof Error) {
//                throw (Error)t;
//            }
//        }
//    }
//
//    /**
//     * Gets event count.
//     *
//     * @param type Event type.
//     * @return Count.
//     */
//    private int getEventCount(int type) {
//        assert type > 0;
//
//        int gridCnt = gridCount();
//
//        int cnt = 0;
//
//        for (int i = 0; i < gridCnt; i++) {
//            cnt += lsnrs[i].getEventCount(type);
//        }
//
//        return cnt;
//    }
//
//    /**
//     * Reset registered counters.
//     */
//    private void resetEventCounters() {
//        for (GridTestEventListener lsnr : lsnrs) {
//            lsnr.resetEventCounters();
//        }
//    }
//
//    /**
//     * Get key-value pairs for cache runnable.
//     *
//     * @param size Pairs count.
//     * @param idx Cache runnable index.
//     * @return Key-value pairs.
//     */
//    private Map<String, Integer> threadPairs(int size, int idx) {
//        Map<String, Integer> pairs = new HashMap<>(size);
//
//        for (int i = 1; i <= size; i++) {
//            pairs.put(KEY + i + idx, (1 + idx) * i);
//        }
//
//        return pairs;
//    }
//
//    /**
//     * Get key-value pairs.
//     *
//     * @param size Pairs count.
//     * @return Key-value pairs.
//     */
//    private Map<String, Integer> commonPairs(int size) {
//        Map<String, Integer> pairs = new HashMap<>(size);
//
//        for (int i = 1; i <= size; i++) {
//            pairs.put(KEY + i, i);
//        }
//
//        return pairs;
//    }
//
//    /**
//     *
//     */
//    private static interface GridTestCacheRunnable {
//        /**
//         * @param cache Cache.
//         * @param idx Index.
//         * @throws Exception If any exception occurs.
//         */
//        void run(Cache<String, Integer> cache, int idx) throws Exception;
//    }
//
//    /**
//     * Local event listener.
//     */
//    private class GridTestEventListener implements GridLocalEventListener {
//        /** Index. */
//        private int idx = -1;
//
//        /** Events count map. */
//        private ConcurrentMap<Integer, AtomicInteger> cntrs = new ConcurrentHashMap<>();
//
//        /**
//         * Creates listener.
//         *
//         * @param idx Index.
//         */
//        private GridTestEventListener(int idx) {
//            this.idx = idx;
//        }
//
//        /** {@inheritDoc} */
//        @SuppressWarnings({"NakedNotify"})
//        @Override public void onEvent(GridEvent evt) {
//            assert evt instanceof GridCacheEvent;
//
//            AtomicInteger cntr = F.addIfAbsent(cntrs, evt.type(), F.newAtomicInt());
//
//            assert cntr != null;
//
//            cntr.incrementAndGet();
//
//            synchronized (mux) {
//                mux.notifyAll();
//            }
//        }
//
//        /**
//         * Gets event count.
//         *
//         * @param type Event type.
//         * @return Count.
//         */
//        private int getEventCount(int type) {
//            AtomicInteger cntr = cntrs.get(type);
//
//            return cntr != null ? cntr.get() : 0;
//        }
//
//        /**
//         * Reset registered counters.
//         */
//        private void resetEventCounters() {
//            for (AtomicInteger cntr : cntrs.values()) {
//                cntr.set(0);
//            }
//        }
//
//        /**
//         * Waits for event count.
//         *
//         * @param evtCnts Tuples with values: V1 - event type, V2 - expected event count.
//         * @throws InterruptedException If thread has been interrupted while waiting.
//         */
//        private void waitForEventCount(GridBiTuple<Integer, Integer>... evtCnts) throws InterruptedException {
//            waitForEventCount(F.asList(evtCnts));
//        }
//
//        /**
//         * Waits for event count.
//         *
//         * @param evtCnts Tuples with values: V1 - event type, V2 - expected event count.
//         * @throws InterruptedException If thread has been interrupted while waiting.
//         */
//        @SuppressWarnings({"UnconditionalWait"})
//        private void waitForEventCount(Collection<GridBiTuple<Integer, Integer>> evtCnts) throws InterruptedException {
//            System.out.println(idx + "|" + evtCnts);
//
//            if (F.isEmpty(evtCnts)) {
//                return;
//            }
//
//            // V1 - event type, V2 - expected event count, V3 - actual event count.
//            Collection<GridTuple3<Integer, Integer, Integer>> c
//                = new ArrayList<>(evtCnts.size());
//
//            F.transform(c, evtCnts,
//                new C1<GridBiTuple<Integer, Integer>, GridTuple3<Integer, Integer, Integer>>() {
//                    @Override public GridTuple3<Integer, Integer, Integer> apply(GridBiTuple<Integer, Integer> t) {
//                        return F.t(t.get1(), t.get2(), -1);
//                    }
//                }
//            );
//
//            long timeout = 1000;
//
//            long threshold = System.currentTimeMillis() + WAIT_TIMEOUT;
//
//            while (System.currentTimeMillis() < threshold) {
//                for(Iterator<GridTuple3<Integer, Integer, Integer>> iter = c.iterator(); iter.hasNext(); ) {
//                    GridTuple3<Integer, Integer, Integer> t = iter.next();
//
//                    int evtType = t.get1();
//                    int evtCnt = t.get2();
//
//                    assert evtType > 0;
//                    assert evtCnt > 0;
//
//                    int actEvtCnt = getEventCount(evtType);
//
//                    System.out.println(idx + "|" + evtType + "|" + actEvtCnt);
//
//                    if (actEvtCnt >= evtCnt) {
//                        iter.remove();
//                    }
//                    else {
//                        t.set3(actEvtCnt);
//                    }
//                }
//
//                if (c.isEmpty()) {
//                    break;
//                }
//
//                synchronized (mux) {
//                    mux.wait(timeout);
//                }
//            }
//
//            if (!c.isEmpty()) {
//                for (GridTuple3<Integer, Integer, Integer> t : c) {
//                    error("Found unexpected event count: [index=" + idx + ", type=" + t.get1() +
//                        ", expectedCnt=" + t.get2() + ", actualCnt=" + t.get3() + ']');
//                }
//
//                assert false;
//            }
//        }
//    }
//
//    /**
//     * Closure for this test.
//     */
//    private static class GridCacheClosure extends GridInClosure<Entry<String, Integer>> {
//        /** 0 - calculates minimum, 1 - maximum, 2 - sum. */
//        private int type;
//
//        /** */
//        private final AtomicInteger num = new AtomicInteger();
//
//        /**
//         *
//         */
//        private GridCacheClosure() {
//            this(0);
//        }
//
//        /**
//         * @param type Type.
//         */
//        private GridCacheClosure(int type) {
//            this.type = type;
//
//            reset();
//        }
//
//        /** {@inheritDoc} */
//        @Override public void apply(Entry<String, Integer> e) {
//            Integer i = e.peek();
//
//            if (i != null) {
//                if (type == 0) {
//                    min(i);
//                }
//                else if (type == 1) {
//                    max(i);
//                }
//                else if (type == 2) {
//                    sum(i);
//                }
//                else {
//                    assert false;
//                }
//            }
//        }
//
//        /**
//         * @param val Entry value.
//         */
//        private void min(int val) {
//            int i = num.get();
//
//            if (val < i) {
//                assert num.compareAndSet(i, val);
//            }
//        }
//
//        /**
//         * @param val Entry value.
//         */
//        private void max(int val) {
//            int i = num.get();
//
//            if (val > i) {
//                assert num.compareAndSet(i, val);
//            }
//        }
//
//        /**
//         * @param val Entry value.
//         */
//        private void sum(int val) {
//            num.addAndGet(val);
//        }
//
//        /**
//         * @return Calculated value.
//         */
//        private int getCalculatedValue() {
//            return num.get();
//        }
//
//        /**
//         *
//         */
//        private void reset() {
//            if (type == 0) {
//                num.set(Integer.MAX_VALUE);
//            }
//            else if (type == 1) {
//                num.set(Integer.MIN_VALUE);
//            }
//            else if (type == 2) {
//                num.set(0);
//            }
//        }
//    }
}