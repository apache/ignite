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

package org.apache.ignite.internal.processors.cache.store;

import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheTestStore;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Basic store test.
 */
public abstract class GridCacheWriteBehindStoreAbstractTest extends GridCommonAbstractTest {
    /** Flush frequency. */
    private static final int WRITE_FROM_BEHIND_FLUSH_FREQUENCY = 1000;

    /** Cache store. */
    private static final GridCacheTestStore store = new GridCacheTestStore();

    /**
     *
     */
    protected GridCacheWriteBehindStoreAbstractTest() {
        super(true /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        store.resetTimestamp();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        IgniteCache<?, ?> cache = jcache();

        if (cache != null)
            cache.clear();

        store.reset();
    }

    /** @return Caching mode. */
    protected abstract CacheMode cacheMode();

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected final IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration c = super.getConfiguration();

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        c.setDiscoverySpi(disco);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(cacheMode());
        cc.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cc.setSwapEnabled(false);
        cc.setAtomicityMode(TRANSACTIONAL);

        cc.setCacheStoreFactory(singletonFactory(store));
        cc.setReadThrough(true);
        cc.setWriteThrough(true);
        cc.setLoadPreviousValue(true);

        cc.setWriteBehindEnabled(true);
        cc.setWriteBehindFlushFrequency(WRITE_FROM_BEHIND_FLUSH_FREQUENCY);

        c.setCacheConfiguration(cc);

        return c;
    }

    /** @throws Exception If test fails. */
    public void testWriteThrough() throws Exception {
        IgniteCache<Integer, String> cache = jcache();

        Map<Integer, String> map = store.getMap();

        assert map.isEmpty();

        Transaction tx = grid().transactions().txStart(OPTIMISTIC, REPEATABLE_READ);

        try {
            for (int i = 1; i <= 10; i++) {
                cache.put(i, Integer.toString(i));

                checkLastMethod(null);
            }

            tx.commit();
        }
        finally {
            tx.close();
        }

        // Need to wait WFB flush timeout.
        U.sleep(WRITE_FROM_BEHIND_FLUSH_FREQUENCY + 100);

        checkLastMethod("putAll");

        assert cache.size() == 10;

        for (int i = 1; i <= 10; i++) {
            String val = map.get(i);

            assert val != null;
            assert val.equals(Integer.toString(i));
        }

        store.resetLastMethod();

        tx = grid().transactions().txStart();

        try {
            for (int i = 1; i <= 10; i++) {
                String val = cache.getAndRemove(i);

                checkLastMethod(null);

                assert val != null;
                assert val.equals(Integer.toString(i));
            }

            tx.commit();
        }
        finally {
            tx.close();
        }

        // Need to wait WFB flush timeout.
        U.sleep(WRITE_FROM_BEHIND_FLUSH_FREQUENCY + 100);

        checkLastMethod("removeAll");

        assert map.isEmpty();
    }

    /** @throws Exception If test failed. */
    public void testReadThrough() throws Exception {
        IgniteCache<Integer, String> cache = jcache();

        Map<Integer, String> map = store.getMap();

        assert map.isEmpty();

        try (Transaction tx = grid().transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {
            for (int i = 1; i <= 10; i++)
                cache.put(i, Integer.toString(i));

            checkLastMethod(null);

            tx.commit();
        }

        // Need to wait WFB flush timeout.
        U.sleep(WRITE_FROM_BEHIND_FLUSH_FREQUENCY + 100);

        checkLastMethod("putAll");

        for (int i = 1; i <= 10; i++) {
            String val = map.get(i);

            assert val != null;
            assert val.equals(Integer.toString(i));
        }

        cache.clear();

        assert cache.localSize() == 0;
        assert cache.localSize() == 0;

        // Need to wait WFB flush timeout.
        U.sleep(WRITE_FROM_BEHIND_FLUSH_FREQUENCY + 100);

        assert map.size() == 10;

        for (int i = 1; i <= 10; i++) {
            // Read through.
            String val = cache.get(i);

            checkLastMethod("load");

            assert val != null;
            assert val.equals(Integer.toString(i));
        }

        assert cache.size() == 10;

        cache.clear();

        assert cache.localSize() == 0;
        assert cache.localSize() == 0;

        assert map.size() == 10;

        Set<Integer> keys = new HashSet<>();

        for (int i = 1; i <= 10; i++)
            keys.add(i);

        // Read through.
        Map<Integer, String> vals = cache.getAll(keys);

        checkLastMethod("loadAll");

        assert vals != null;
        assert vals.size() == 10;

        for (int i = 1; i <= 10; i++) {
            String val = vals.get(i);

            assert val != null;
            assert val.equals(Integer.toString(i));
        }

        // Write through.
        cache.removeAll(keys);

        // Need to wait WFB flush timeout.
        U.sleep(WRITE_FROM_BEHIND_FLUSH_FREQUENCY + 100);

        checkLastMethod("removeAll");

        assert cache.localSize() == 0;
        assert cache.localSize() == 0;

        assert map.isEmpty();
    }

    /** @throws Exception If failed. */
    public void testMultithreaded() throws Exception {
        final ConcurrentMap<String, Set<Integer>> perThread = new ConcurrentHashMap<>();

        final AtomicBoolean running = new AtomicBoolean(true);

        final IgniteCache<Integer, String> cache = jcache();

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @SuppressWarnings({"NullableProblems"})
            @Override public void run() {
                // Initialize key set for this thread.
                Set<Integer> set = new HashSet<>();

                Set<Integer> old = perThread.putIfAbsent(Thread.currentThread().getName(), set);

                if (old != null)
                    set = old;

                Random rnd = new Random();

                int keyCnt = 20000;

                while (running.get()) {
                    int op = rnd.nextInt(2);
                    int key = rnd.nextInt(keyCnt);

                    switch (op) {
                        case 0:
                            cache.put(key, "val" + key);
                            set.add(key);

                            break;

                        case 1:
                        default:
                            cache.remove(key);
                            set.remove(key);

                            break;
                    }
                }
            }
        }, 10, "put");

        U.sleep(10000);

        running.set(false);

        fut.get();

        U.sleep(5 * WRITE_FROM_BEHIND_FLUSH_FREQUENCY);

        Map<Integer, String> stored = store.getMap();

        for (Map.Entry<Integer, String> entry : stored.entrySet()) {
            int key = entry.getKey();

            assertEquals("Invalid value for key " + key, "val" + key, entry.getValue());

            boolean found = false;

            for (Set<Integer> threadPuts : perThread.values()) {
                if (threadPuts.contains(key)) {
                    found = true;

                    break;
                }
            }

            assert found : "No threads found that put key " + key;
        }
    }

    /** @param mtd Expected last method value. */
    private void checkLastMethod(@Nullable String mtd) {
        String lastMtd = store.getLastMethod();

        if (mtd == null)
            assert lastMtd == null : "Last method must be null: " + lastMtd;
        else {
            assert lastMtd != null : "Last method must be not null";
            assert lastMtd.equals(mtd) : "Last method does not match [expected=" + mtd + ", lastMtd=" + lastMtd + ']';
        }
    }

}