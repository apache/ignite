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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.P2;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Basic store test.
 */
public abstract class GridCacheBasicStoreAbstractTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Cache store. */
    private static final GridCacheTestStore store = new GridCacheTestStore();

    /**
     *
     */
    protected GridCacheBasicStoreAbstractTest() {
        super(true /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        store.resetTimestamp();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        jcache().clear();

        store.reset();
    }

    /** @return Caching mode. */
    protected abstract CacheMode cacheMode();

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected final IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        c.setDiscoverySpi(disco);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(cacheMode());
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setSwapEnabled(false);
        cc.setAtomicityMode(atomicityMode());
        cc.setRebalanceMode(SYNC);

        cc.setCacheStoreFactory(singletonFactory(store));
        cc.setReadThrough(true);
        cc.setWriteThrough(true);
        cc.setLoadPreviousValue(true);

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * @return Cache atomicity mode.
     */
    protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testNotExistingKeys() throws IgniteCheckedException {
        IgniteCache<Integer, String> cache = jcache();
        Map<Integer, String> map = store.getMap();

        cache.put(100, "hacuna matata");
        assertEquals(1, map.size());

        cache.localEvict(Collections.singleton(100));
        assertEquals(1, map.size());

        assertEquals("hacuna matata", cache.getAndRemove(100));
        assertTrue(map.isEmpty());

        store.resetLastMethod();
        assertNull(store.getLastMethod());

        cache.remove(200);
        assertEquals("remove", store.getLastMethod());

        cache.get(300);
        assertEquals("load", store.getLastMethod());
    }

    /** @throws Exception If test fails. */
    public void testWriteThrough() throws Exception {
        IgniteCache<Integer, String> cache = jcache();

        Map<Integer, String> map = store.getMap();

        assert map.isEmpty();

        if (atomicityMode() == TRANSACTIONAL) {
            try (Transaction tx = grid().transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {
                for (int i = 1; i <= 10; i++) {
                    cache.put(i, Integer.toString(i));

                    checkLastMethod(null);
                }

                tx.commit();
            }
        }
        else {
            Map<Integer, String> putMap = new HashMap<>();

            for (int i = 1; i <= 10; i++)
                putMap.put(i, Integer.toString(i));

            cache.putAll(putMap);
        }

        checkLastMethod("putAll");

        assert cache.size() == 10;

        for (int i = 1; i <= 10; i++) {
            String val = map.get(i);

            assert val != null;
            assert val.equals(Integer.toString(i));
        }

        store.resetLastMethod();

        if (atomicityMode() == TRANSACTIONAL) {
            try (Transaction tx = grid().transactions().txStart()) {
                for (int i = 1; i <= 10; i++) {
                    String val = cache.getAndRemove(i);

                    checkLastMethod(null);

                    assert val != null;
                    assert val.equals(Integer.toString(i));
                }

                tx.commit();

                checkLastMethod("removeAll");
            }
        }
        else {
            Set<Integer> keys = new HashSet<>();

            for (int i = 1; i <= 10; i++)
                keys.add(i);

            cache.removeAll(keys);

            checkLastMethod("removeAll");
        }

        assert map.isEmpty();
    }

    /** @throws Exception If test failed. */
    public void testReadThrough() throws Exception {
        IgniteCache<Integer, String> cache = jcache();

        Map<Integer, String> map = store.getMap();

        assert map.isEmpty();

        if (atomicityMode() == TRANSACTIONAL) {
            try (Transaction tx = grid().transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {
                for (int i = 1; i <= 10; i++)
                    cache.put(i, Integer.toString(i));

                checkLastMethod(null);

                tx.commit();
            }
        }
        else {
            Map<Integer, String> putMap = new HashMap<>();

            for (int i = 1; i <= 10; i++)
                putMap.put(i, Integer.toString(i));

            cache.putAll(putMap);
        }

        checkLastMethod("putAll");

        for (int i = 1; i <= 10; i++) {
            String val = map.get(i);

            assert val != null;
            assert val.equals(Integer.toString(i));
        }

        cache.clear();

        assert cache.localSize() == 0;
        assert cache.localSize() == 0;

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

        checkLastMethod("removeAll");

        assert cache.localSize() == 0;
        assert cache.localSize() == 0;

        assert map.isEmpty();
    }

    /** @throws Exception If test failed. */
    public void testLoadCache() throws Exception {
        IgniteCache<Integer, String> cache = jcache();

        int cnt = 1;

        cache.loadCache(null, cnt);

        checkLastMethod("loadAllFull");

        assert !(cache.localSize() == 0);

        Map<Integer, String> map = cache.getAll(keySet(cache));

        assert map.size() == cnt : "Invalid map size: " + map.size();

        // Recheck last method to make sure
        // values were read from cache.
        checkLastMethod("loadAllFull");

        int start = store.getStart();

        for (int i = start; i < start + cnt; i++) {
            String val = map.get(i);

            assert val != null;
            assert val.equals(Integer.toString(i));
        }
    }

    /** @throws Exception If test failed. */
    public void testLoadCacheWithPredicate() throws Exception {
        IgniteCache<Integer, String> cache = jcache();

        int cnt = 10;

        cache.loadCache(new P2<Integer, String>() {
            @Override public boolean apply(Integer key, String val) {
                // Accept only even numbers.
                return key % 2 == 0;
            }
        }, cnt);

        checkLastMethod("loadAllFull");

        Map<Integer, String> map = cache.getAll(keySet(cache));

        assert map.size() == cnt / 2 : "Invalid map size: " + map.size();

        // Recheck last method to make sure
        // values were read from cache.
        checkLastMethod("loadAllFull");

        int start = store.getStart();

        for (int i = start; i < start + cnt; i++) {
            String val = map.get(i);

            if (i % 2 == 0) {
                assert val != null;
                assert val.equals(Integer.toString(i));
            }
            else
                assert val == null;
        }
    }

    /** @throws Exception If test failed. */
    public void testReloadCache() throws Exception {
        IgniteCache<Integer, String> cache = jcache();

        cache.loadCache(null, 0);

        assert cache.size() == 0;

        checkLastMethod("loadAllFull");

        Set<Integer> keys = new HashSet<>();

        for (int i = 1; i <= 10; i++) {
            keys.add(i);

            cache.put(i, Integer.toString(i));

            checkLastMethod("put");
        }

        assert cache.size() == 10;

        loadAll(cache, keys, true);

        checkLastMethod("loadAll");

        assert cache.size() == 10;

        store.resetLastMethod();

        for (int i = 1; i <= 10; i++) {
            String val = cache.get(i);

            assert val != null;
            assert val.equals(Integer.toString(i));

            // Make sure that value is coming from cache, not from store.
            checkLastMethod(null);
        }

        cache.clear();

        cache.loadCache(new P2<Integer, String>() {
            @Override public boolean apply(Integer k, String v) {
                // Only accept even numbers.
                return k % 2 == 0;
            }
        }, 10);

        checkLastMethod("loadAllFull");

        store.resetLastMethod();

        assertEquals(5, cache.size());

        for (Cache.Entry<Integer, String> entry : cache) {
            String val = entry.getValue();

            assert val != null;
            assert val.equals(Integer.toString(entry.getKey()));
            assert entry.getKey() % 2 == 0;

            // Make sure that value is coming from cache, not from store.
            checkLastMethod(null);
        }

        // Make sure that value is coming from cache, not from store.
        checkLastMethod(null);
    }

    /** @throws Exception If test failed. */
    public void testReloadAll() throws Exception {
        IgniteCache<Integer, String> cache = jcache();

        assert cache.size() == 0;

        Map<Integer, String> vals = new HashMap<>();

        for (int i = 1; i <= 10; i++)
            vals.put(i, Integer.toString(i));

        loadAll(cache, vals.keySet(), true);

        assert cache.size() == 0: "Cache is not empty.";

        checkLastMethod("loadAll");

        cache.putAll(vals);

        checkLastMethod("putAll");

        assert cache.size() == 10;

        loadAll(cache, vals.keySet(), true);

        checkLastMethod("loadAll");

        assert cache.size() == 10;

        store.resetLastMethod();

        for (int i = 1; i <= 10; i++) {
            String val = cache.get(i);

            assert val != null;
            assert val.equals(Integer.toString(i));

            // Make sure that value is coming from cache, not from store.
            checkLastMethod(null);
        }

        for (int i = 1; i <= 10; i++)
            store.write(new CacheEntryImpl<>(i, "reloaded-" + i));

        loadAll(cache, vals.keySet(), true);

        checkLastMethod("loadAll");

        store.resetLastMethod();

        assert cache.size() == 10;

        for (int i = 1; i <= 10; i++) {
            String val = cache.get(i);

            assert val != null;
            assert val.equals("reloaded-" + i);

            // Make sure that value is coming from cache, not from store.
            checkLastMethod(null);
        }
    }

    /** @throws Exception If test failed. */
    @SuppressWarnings("StringEquality")
    public void testReload() throws Exception {
        IgniteCache<Integer, String> cache = jcache();

        assert cache.size() == 0;

        Map<Integer, String> vals = new HashMap<>();

        for (int i = 1; i <= 10; i++)
            vals.put(i, Integer.toString(i));

        loadAll(cache, vals.keySet(), true);

        assert cache.size() == 0;

        checkLastMethod("loadAll");

        cache.putAll(vals);

        checkLastMethod("putAll");

        assert cache.size() == 10;

        load(cache, 1, true);

        String val = cache.localPeek(1, CachePeekMode.ONHEAP);

        assert val != null;
        assert "1".equals(val);

        checkLastMethod("load");

        assert cache.size() == 10;

        store.resetLastMethod();

        for (int i = 1; i <= 10; i++) {
            val = cache.get(i);

            assert val != null;
            assert val.equals(Integer.toString(i));

            // Make sure that value is coming from cache, not from store.
            checkLastMethod(null);
        }

        for (int i = 1; i <= 10; i++)
            store.write(new CacheEntryImpl<>(i, "reloaded-" + i));

        store.resetLastMethod();

        assert cache.size() == 10;

        for (int i = 1; i <= 10; i++) {
            load(cache, i, true);

            val = cache.localPeek(i, CachePeekMode.ONHEAP);

            checkLastMethod("load");

            assertEquals("reloaded-" + i, val);

            store.resetLastMethod();

            String cached = cache.get(i);

            assert cached != null;

            assert cached.equals(val) : "Cached value mismatch [expected=" + val + ", cached=" + cached + ']';

            // Make sure that value is coming from cache, not from store.
            checkLastMethod(null);
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