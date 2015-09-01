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

package org.apache.ignite.internal.processors.cache;

import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.CA;
import org.apache.ignite.internal.util.typedef.CAX;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CIX1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.GridTestUtils;

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
    private void runTest(final IgniteInClosure<IgniteCache<String, Integer>> c) throws Exception {
        final IgniteInternalFuture<?> fut1 = GridTestUtils.runMultiThreadedAsync(new CAX() {
            @Override public void applyx() {
                while (true) {
                    int i = cnt.getAndIncrement();

                    if (i >= PUT_CNT)
                        break;

                    jcache().put("key" + i, i);

                    set.add(i);

                    if (i > 10)
                        guard.compareAndSet(false, true);
                }
            }
        }, WRITE_THREAD_CNT, WRITE_THREAD_NAME);

        IgniteInternalFuture<?> fut2 = GridTestUtils.runMultiThreadedAsync(new CA() {
            @Override public void apply() {
                IgniteCache<String, Integer> cache = jcache();

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
     *
     */
    private void checkConsistency() {
        for (Cache.Entry<String, Integer> e : jcache())
            for (int i = 1; i < gridCount(); i++) {
                Integer val = jcache(i).get(e.getKey());

                if (val == null)
                    assert e.getValue() == null;
                else
                    assert val.equals(e.getValue());
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
    private Set<String> rangeKeys(int fromIncl, int toExcl) {
        return new HashSet<>(F.transform(F.range(fromIncl, toExcl), new C1<Integer, String>() {
            @Override public String apply(Integer i) {
                return "key" + i;
            }
        }));
    }

    /**
     * @throws Exception In case of error.
     */
    public void testContainsKey() throws Exception {
        runTest(new CI1<IgniteCache<String,Integer>>() {
            @Override public void apply(IgniteCache<String, Integer> cache) {
                assert cache.containsKey("key" + random());
                assert !cache.containsKey("wrongKey");
            }
        });
    }

    /**
     * @throws Exception In case of error.
     */
    public void testGet() throws Exception {
        runTest(new CIX1<IgniteCache<String,Integer>>() {
            @Override public void applyx(IgniteCache<String, Integer> cache) {
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
        runTest(new CIX1<IgniteCache<String,Integer>>() {
            @Override public void applyx(IgniteCache<String, Integer> cache) {
                int rnd = random();

                IgniteCache<String, Integer> cacheAsync = cache.withAsync();

                cacheAsync.get("key" + rnd);

                assert cacheAsync.<Integer>future().get() == rnd;

                cache.get("wrongKey");

                assert cacheAsync.future().get() == null;
            }
        });
    }

    /**
     * @throws Exception In case of error.
     */
    public void testGetAll() throws Exception {
        runTest(new CIX1<IgniteCache<String,Integer>>() {
            @Override public void applyx(IgniteCache<String, Integer> cache) {
                int rnd1 = random();
                int rnd2 = random();

                Map<String, Integer> map = cache.getAll(ImmutableSet.of("key" + rnd1, "key" + rnd2));

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
        runTest(new CIX1<IgniteCache<String,Integer>>() {
            @Override public void applyx(IgniteCache<String, Integer> cache) {
                int rnd1 = random();
                int rnd2 = random();

                cache.withAsync().getAll(ImmutableSet.of("key" + rnd1, "key" + rnd2));
                Map<String, Integer> map = cache.withAsync().<Map<String, Integer>>future().get();

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
        runTest(new CIX1<IgniteCache<String,Integer>>() {
            @Override public void applyx(IgniteCache<String, Integer> cache) {
                int rnd1 = random();
                int rnd2 = random();

                assert cache.getAndRemove("wrongKey") == null;
                assert !cache.remove("key" + rnd1, -1);

                assert cache.localPeek("key" + rnd1, CachePeekMode.ONHEAP) == null || cache.localPeek("key" + rnd1, CachePeekMode.ONHEAP) == rnd1;
                assert cache.localPeek("key" + rnd2, CachePeekMode.ONHEAP) == null || cache.localPeek("key" + rnd2, CachePeekMode.ONHEAP) == rnd2;

                assert cache.localPeek("key" + rnd1, CachePeekMode.ONHEAP) == null || cache.getAndRemove("key" + rnd1) == rnd1;
                assert cache.localPeek("key" + rnd2, CachePeekMode.ONHEAP) == null || cache.remove("key" + rnd2, rnd2);

                assert cache.localPeek("key" + rnd1, CachePeekMode.ONHEAP) == null;
                assert cache.localPeek("key" + rnd2, CachePeekMode.ONHEAP) == null;
            }
        });
    }

    /**
     * @throws Exception In case of error.
     */
    public void testRemoveAsync() throws Exception {
        runTest(new CIX1<IgniteCache<String,Integer>>() {
            @Override public void applyx(IgniteCache<String, Integer> cache) {
                int rnd1 = random();
                int rnd2 = random();

                IgniteCache<String, Integer> cacheAsync = cache.withAsync();

                cacheAsync.getAndRemove("wrongKey");

                assert cacheAsync.future().get() == null;

                cacheAsync.remove("key" + rnd1, -1);

                assert !cacheAsync.<Boolean>future().get();

                assert cache.localPeek("key" + rnd1, CachePeekMode.ONHEAP) == null || cache.localPeek("key" + rnd1, CachePeekMode.ONHEAP) == rnd1;
                assert cache.localPeek("key" + rnd2, CachePeekMode.ONHEAP) == null || cache.localPeek("key" + rnd2, CachePeekMode.ONHEAP) == rnd2;

                assert cache.localPeek("key" + rnd1, CachePeekMode.ONHEAP) == null || removeAsync(cache, "key" + rnd1) == rnd1;
                assert cache.localPeek("key" + rnd2, CachePeekMode.ONHEAP) == null || removeAsync(cache, "key" + rnd2, rnd2);

                assert cache.localPeek("key" + rnd1, CachePeekMode.ONHEAP) == null;
                assert cache.localPeek("key" + rnd2, CachePeekMode.ONHEAP) == null;
            }
        });
    }

    /**
     * @throws Exception In case of error.
     */
    public void testRemoveAll() throws Exception {
        runTest(new CIX1<IgniteCache<String,Integer>>() {
            @Override public void applyx(IgniteCache<String, Integer> cache) {
                int rnd = random();

                cache.removeAll(rangeKeys(0, rnd));

                for (int i = 0; i < rnd; i++)
                    assert cache.localPeek("key" + i, CachePeekMode.ONHEAP) == null;
            }
        });
    }

    /**
     * @throws Exception In case of error.
     */
    public void testRemoveAllAsync() throws Exception {
        runTest(new CIX1<IgniteCache<String,Integer>>() {
            @Override public void applyx(IgniteCache<String, Integer> cache) {
                int rnd = random();

                IgniteCache<String, Integer> cacheAsync = cache.withAsync();

                cacheAsync.removeAll(rangeKeys(0, rnd));

                cacheAsync.future().get();

                for (int i = 0; i < rnd; i++)
                    assert cache.localPeek("key" + i, CachePeekMode.ONHEAP) == null;
            }
        });
    }

    /**
     * @param cache Cache.
     * @param key Key.
     */
    private <K, V> V removeAsync(IgniteCache<K, V> cache, K key) {
        IgniteCache<K, V> cacheAsync = cache.withAsync();

        cacheAsync.getAndRemove(key);

        return cacheAsync.<V>future().get();
    }

    /**
     * @param cache Cache.
     * @param key Key.
     */
    private <K, V> boolean removeAsync(IgniteCache<K, V> cache, K key, V val) {
        IgniteCache<K, V> cacheAsync = cache.withAsync();

        cacheAsync.remove(key, val);

        return cacheAsync.<Boolean>future().get();
    }
}