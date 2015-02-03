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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;

import javax.cache.configuration.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.transactions.IgniteTxConcurrency.*;
import static org.apache.ignite.transactions.IgniteTxIsolation.*;

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
        GridCache<?, ?> cache = cache();

        if (cache != null)
            cache.clearAll();

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
        cc.setDistributionMode(NEAR_PARTITIONED);

        cc.setCacheStoreFactory(new FactoryBuilder.SingletonFactory(store));
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
        GridCache<Integer, String> cache = cache();

        Map<Integer, String> map = store.getMap();

        assert map.isEmpty();

        IgniteTx tx = cache.txStart(OPTIMISTIC, REPEATABLE_READ);

        try {
            for (int i = 1; i <= 10; i++) {
                cache.putx(i, Integer.toString(i));

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

        tx = cache.txStart();

        try {
            for (int i = 1; i <= 10; i++) {
                String val = cache.remove(i);

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
        GridCache<Integer, String> cache = cache();

        Map<Integer, String> map = store.getMap();

        assert map.isEmpty();

        try (IgniteTx tx = cache.txStart(OPTIMISTIC, REPEATABLE_READ)) {
            for (int i = 1; i <= 10; i++)
                cache.putx(i, Integer.toString(i));

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

        cache.clearAll();

        assert cache.isEmpty();
        assert cache.isEmpty();

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

        cache.clearAll();

        assert cache.isEmpty();
        assert cache.isEmpty();

        assert map.size() == 10;

        Collection<Integer> keys = new ArrayList<>();

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

        assert cache.isEmpty();
        assert cache.isEmpty();

        assert map.isEmpty();
    }

    /** @throws Exception If failed. */
    public void testMultithreaded() throws Exception {
        final ConcurrentMap<String, Set<Integer>> perThread = new ConcurrentHashMap<>();

        final AtomicBoolean running = new AtomicBoolean(true);

        final GridCache<Integer, String> cache = cache();

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @SuppressWarnings({"NullableProblems"})
            @Override public void run() {
                // Initialize key set for this thread.
                Set<Integer> set = new HashSet<>();

                Set<Integer> old = perThread.putIfAbsent(Thread.currentThread().getName(), set);

                if (old != null)
                    set = old;

                Random rnd = new Random();

                try {
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
                catch (IgniteCheckedException e) {
                    error("Unexpected exception in put thread", e);

                    assert false;
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
