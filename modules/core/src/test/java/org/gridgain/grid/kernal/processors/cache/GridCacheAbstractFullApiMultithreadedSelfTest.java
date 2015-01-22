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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;

import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Multithreaded cache API tests.
 */
public abstract class GridCacheAbstractFullApiMultithreadedSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final Random RND = new Random();

    /** */
    private static final int WRITE_THREAD_CNT = 3;

    /** */
    private static final int READ_THREAD_CNT = 3;

    /** */
    private static final String WRITE_THREAD_NAME = "write-thread";

    /** */
    private static final String READ_THREAD_NAME = "read-thread";

    /** */
    private static final int PUT_CNT = 100;

    /** */
    private final AtomicInteger cnt = new AtomicInteger();

    /** */
    private final AtomicBoolean guard = new AtomicBoolean();

    /** */
    private final Collection<Integer> set = new GridConcurrentHashSet<>();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cnt.set(0);
        guard.set(false);
        set.clear();
    }

    /**
     * @param c Test closure.
     * @throws Exception In case of error.
     */
    private void runTest(final IgniteInClosure<GridCache<String, Integer>> c) throws Exception {
        final IgniteFuture<?> fut1 = GridTestUtils.runMultiThreadedAsync(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                while (true) {
                    int i = cnt.getAndIncrement();

                    if (i >= PUT_CNT)
                        break;

                    cache().put("key" + i, i);

                    set.add(i);

                    if (i > 10)
                        guard.compareAndSet(false, true);
                }
            }
        }, WRITE_THREAD_CNT, WRITE_THREAD_NAME);

        IgniteFuture<?> fut2 = GridTestUtils.runMultiThreadedAsync(new CA() {
            @Override public void apply() {
                GridCache<String, Integer> cache = cache();

                while (!fut1.isDone())
                    if (guard.get())
                        c.apply(cache);
            }
        }, READ_THREAD_CNT, READ_THREAD_NAME);

        fut1.get();
        fut2.get();

        checkConsistency();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void checkConsistency() throws IgniteCheckedException {
        for (GridCacheEntry<String, Integer> e : cache())
            for (int i = 1; i < gridCount(); i++) {
                Integer val = cache(i).get(e.getKey());

                if (val == null)
                    assert e.get() == null;
                else
                    assert val.equals(e.get());
            }
    }

    /**
     * @return Random.
     */
    private int random() {
        int rnd;

        do
            rnd = RND.nextInt(PUT_CNT);
        while (!set.contains(rnd));

        return rnd;
    }

    /**
     * @param fromIncl Inclusive start of the range.
     * @param toExcl Exclusive stop of the range.
     * @return Range of keys.
     */
    private Collection<String> rangeKeys(int fromIncl, int toExcl) {
        return F.transform(F.range(fromIncl, toExcl), new C1<Integer, String>() {
            @Override public String apply(Integer i) {
                return "key" + i;
            }
        });
    }

    /**
     * @throws Exception In case of error.
     */
    public void testContainsKey() throws Exception {
        runTest(new CI1<GridCache<String,Integer>>() {
            @Override public void apply(GridCache<String, Integer> cache) {
                assert cache.containsKey("key" + random());
                assert !cache.containsKey("wrongKey");
            }
        });
    }

    /**
     * @throws Exception In case of error.
     */
    public void testContainsKeyFiltered() throws Exception {
        runTest(new CI1<GridCache<String,Integer>>() {
            @Override public void apply(GridCache<String, Integer> cache) {
                assert cache.projection(F.<String, Integer>cacheHasPeekValue()).containsKey("key");
                assert !cache.projection(F.<String, Integer>cacheNoPeekValue()).containsKey("key" + random());
            }
        });
    }

    /**
     * @throws Exception In case of error.
     */
    public void testContainsValue() throws Exception {
        runTest(new CI1<GridCache<String,Integer>>() {
            @Override public void apply(GridCache<String, Integer> cache) {
                assert cache.containsValue(random());
                assert !cache.containsValue(-1);
            }
        });
    }

    /**
     * @throws Exception In case of error.
     */
    public void testContainsValueFiltered() throws Exception {
        runTest(new CI1<GridCache<String,Integer>>() {
            @Override public void apply(GridCache<String, Integer> cache) {
                assert cache.projection(F.<String, Integer>cacheHasPeekValue()).containsValue(random());
                assert !cache.projection(F.<String, Integer>cacheNoPeekValue()).containsValue(random());
            }
        });
    }

    /**
     * @throws Exception In case of error.
     */
    public void testForAll() throws Exception {
        runTest(new CI1<GridCache<String,Integer>>() {
            @Override public void apply(GridCache<String, Integer> cache) {
                assert cache.forAll(new P1<GridCacheEntry<String, Integer>>() {
                    @Override public boolean apply(GridCacheEntry<String, Integer> e) {
                        Integer val = e.peek();

                        return val == null || val <= PUT_CNT;
                    }
                });
            }
        });
    }

    /**
     * @throws Exception In case of error.
     */
    public void testGet() throws Exception {
        runTest(new CIX1<GridCache<String,Integer>>() {
            @Override public void applyx(GridCache<String, Integer> cache) throws IgniteCheckedException {
                int rnd = random();

                assert cache.get("key" + rnd) == rnd;
                assert cache.get("wrongKey") == null;
            }
        });
    }

    /**
     * @throws Exception In case of error.
     */
    public void testGetAsync() throws Exception {
        runTest(new CIX1<GridCache<String,Integer>>() {
            @Override public void applyx(GridCache<String, Integer> cache) throws IgniteCheckedException {
                int rnd = random();

                assert cache.getAsync("key" + rnd).get() == rnd;
                assert cache.getAsync("wrongKey").get() == null;
            }
        });
    }

    /**
     * @throws Exception In case of error.
     */
    public void testGetAll() throws Exception {
        runTest(new CIX1<GridCache<String,Integer>>() {
            @Override public void applyx(GridCache<String, Integer> cache) throws IgniteCheckedException {
                int rnd1 = random();
                int rnd2 = random();

                Map<String, Integer> map = cache.getAll(F.asList("key" + rnd1, "key" + rnd2));

                assert map.size() == (rnd1 != rnd2 ? 2 : 1);
                assert map.get("key" + rnd1) == rnd1;
                assert map.get("key" + rnd2) == rnd2;
            }
        });
    }

   /**
     * @throws Exception In case of error.
     */
    public void testGetAllAsync() throws Exception {
        runTest(new CIX1<GridCache<String,Integer>>() {
            @Override public void applyx(GridCache<String, Integer> cache) throws IgniteCheckedException {
                int rnd1 = random();
                int rnd2 = random();

                Map<String, Integer> map = cache.getAllAsync(F.asList("key" + rnd1, "key" + rnd2)).get();

                assert map.size() == (rnd1 != rnd2 ? 2 : 1);
                assert map.get("key" + rnd1) == rnd1;
                assert map.get("key" + rnd2) == rnd2;
            }
        });
    }

    /**
     * @throws Exception In case of error.
     */
    public void testRemove() throws Exception {
        runTest(new CIX1<GridCache<String,Integer>>() {
            @Override public void applyx(GridCache<String, Integer> cache) throws IgniteCheckedException {
                int rnd1 = random();
                int rnd2 = random();

                assert cache.remove("wrongKey") == null;
                assert !cache.remove("key" + rnd1, -1);

                assert cache.peek("key" + rnd1) == null || cache.peek("key" + rnd1) == rnd1;
                assert cache.peek("key" + rnd2) == null || cache.peek("key" + rnd2) == rnd2;

                assert cache.peek("key" + rnd1) == null || cache.remove("key" + rnd1) == rnd1;
                assert cache.peek("key" + rnd2) == null || cache.remove("key" + rnd2, rnd2);

                assert cache.peek("key" + rnd1) == null;
                assert cache.peek("key" + rnd2) == null;
            }
        });
    }

    /**
     * @throws Exception In case of error.
     */
    public void testRemoveAsync() throws Exception {
        runTest(new CIX1<GridCache<String,Integer>>() {
            @Override public void applyx(GridCache<String, Integer> cache) throws IgniteCheckedException {
                int rnd1 = random();
                int rnd2 = random();

                assert cache.removeAsync("wrongKey").get() == null;
                assert !cache.removeAsync("key" + rnd1, -1).get();

                assert cache.peek("key" + rnd1) == null || cache.peek("key" + rnd1) == rnd1;
                assert cache.peek("key" + rnd2) == null || cache.peek("key" + rnd2) == rnd2;

                assert cache.peek("key" + rnd1) == null || cache.removeAsync("key" + rnd1).get() == rnd1;
                assert cache.peek("key" + rnd2) == null || cache.removeAsync("key" + rnd2, rnd2).get();

                assert cache.peek("key" + rnd1) == null;
                assert cache.peek("key" + rnd2) == null;
            }
        });
    }

    /**
     * @throws Exception In case of error.
     */
    public void testRemoveAll() throws Exception {
        runTest(new CIX1<GridCache<String,Integer>>() {
            @Override public void applyx(GridCache<String, Integer> cache) throws IgniteCheckedException {
                int rnd = random();

                cache.removeAll(rangeKeys(0, rnd));

                for (int i = 0; i < rnd; i++)
                    assert cache.peek("key" + i) == null;
            }
        });
    }

    /**
     * @throws Exception In case of error.
     */
    public void testRemoveAllFiltered() throws Exception {
        runTest(new CIX1<GridCache<String,Integer>>() {
            @Override public void applyx(GridCache<String, Integer> cache) throws IgniteCheckedException {
                final int rnd = random();

                cache.removeAll(new P1<GridCacheEntry<String, Integer>>() {
                    @Override public boolean apply(GridCacheEntry<String, Integer> e) {
                        Integer val = e.peek();

                        return val != null && val < rnd;
                    }
                });

                for (int i = 0; i < rnd; i++)
                    assert cache.peek("key" + i) == null;
            }
        });
    }

    /**
     * @throws Exception In case of error.
     */
    public void testRemoveAllAsync() throws Exception {
        runTest(new CIX1<GridCache<String,Integer>>() {
            @Override public void applyx(GridCache<String, Integer> cache) throws IgniteCheckedException {
                int rnd = random();

                cache.removeAllAsync(rangeKeys(0, rnd)).get();

                for (int i = 0; i < rnd; i++)
                    assert cache.peek("key" + i) == null;
            }
        });
    }

    /**
     * @throws Exception In case of error.
     */
    public void testRemoveAllAsyncFiltered() throws Exception {
        runTest(new CIX1<GridCache<String,Integer>>() {
            @Override public void applyx(GridCache<String, Integer> cache) throws IgniteCheckedException {
                final int rnd = random();

                cache.removeAllAsync(new P1<GridCacheEntry<String, Integer>>() {
                    @Override public boolean apply(GridCacheEntry<String, Integer> e) {
                        Integer val = e.peek();

                        return val != null && val < rnd;
                    }
                }).get();

                for (int i = 0; i < rnd; i++)
                    assert cache.peek("key" + i) == null;
            }
        });
    }
}
