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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Single node test for near cache.
 */
public class GridCacheNearOneNodeSelfTest extends GridCommonAbstractTest {
    /** Cache store. */
    private static TestStore store = new TestStore();

    /**
     *
     */
    public GridCacheNearOneNodeSelfTest() {
        super(true /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        store.reset();

        jcache().removeAll();

        assertEquals("DHT entries: " + dht().entries(), 0, dht().size());
        assertEquals("Near entries: " + near().entries(), 0, near().size());
        assertEquals(0, jcache().size());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(disco);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setBackups(1);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        cacheCfg.setCacheStoreFactory(singletonFactory(store));
        cacheCfg.setReadThrough(true);
        cacheCfg.setWriteThrough(true);
        cacheCfg.setLoadPreviousValue(true);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** @throws Exception If failed. */
    public void testRemove() throws Exception {
        IgniteCache<Object, Object> near = jcache();

        assertEquals("DHT entries: " + dht().entries(), 0, dht().size());
        assertEquals("Near entries: " + near().entries(), 0, near().size());
        assertEquals(0, near.size());

        for (int i = 0; i < 10; i++)
            near.put(i, Integer.toString(i));

        assertEquals("DHT entries: " + dht().entries(), 10, dht().size());
        assertEquals("Near entries: " + near().entries(), 10, near().size());
        assertEquals(10, near.size());

        near.remove(0);

        assertEquals("DHT entries: " + dht().entries(), 9, dht().size());
        assertEquals("Near entries: " + near().entries(), 9, near().size());
        assertEquals(9, near.size());

        near.removeAll();

        assertEquals("DHT entries: " + dht().entries(), 0, dht().size());
        assertEquals("Near entries: " + near().entries(), 0, near().size());
        assertEquals(0, near.size());
    }

    /** @throws Exception If failed. */
    public void testReadThrough() throws Exception {
        IgniteCache<Integer, String> near = jcache();

        GridCacheAdapter<Integer, String> dht = dht();

        String s = near.get(1);

        assert s != null;
        assertEquals(s, "1");

        assertEquals(1, near.size());
        assertEquals(1, near.size());

        String d = localPeek(dht, 1);

        assert d != null;
        assertEquals(d, "1");

        assert dht.size() == 1;
        assert dht.size() == 1;

        assert store.hasValue(1);
    }

    /**
     * Test Optimistic repeatable read write-through.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings({"ConstantConditions"})
    public void testOptimisticTxWriteThrough() throws Exception {
        IgniteCache<Object, Object> near = jcache();
        GridCacheAdapter<Integer, String> dht = dht();

        try (Transaction tx = grid().transactions().txStart(OPTIMISTIC, REPEATABLE_READ) ) {
            near.put(2, "2");
            near.put(3, "3");

            assert "2".equals(near.get(2));
            assert "3".equals(near.get(3));

            GridCacheEntryEx entry = dht.peekEx(2);

            assert entry == null || entry.rawGetOrUnmarshal(false) == null : "Invalid entry: " + entry;

            tx.commit();
        }

        assert "2".equals(near.get(2));
        assert "3".equals(near.get(3));

        assert "2".equals(dht.get(2));
        assert "3".equals(dht.get(3));

        assertEquals(2, near.size());
        assertEquals(2, near.size());

        assertEquals(2, dht.size());
        assertEquals(2, dht.size());
    }

    /** @throws Exception If failed. */
    public void testSingleLockPut() throws Exception {
        IgniteCache<Integer, String> near = jcache();

        Lock lock = near.lock(1);

        lock.lock();

        try {
            near.put(1, "1");
            near.put(2, "2");

            String one = near.getAndPut(1, "3");

            assertNotNull(one);
            assertEquals("1", one);
        }
        finally {
            lock.unlock();
        }
    }

    /** @throws Exception If failed. */
    public void testSingleLock() throws Exception {
        IgniteCache<Integer, String> near = jcache();

        Lock lock = near.lock(1);

        lock.lock();

        try {
            near.put(1, "1");

            assertEquals("1", near.localPeek(1, CachePeekMode.ONHEAP));
            assertEquals("1", dhtPeek(1));

            assertEquals("1", near.get(1));
            assertEquals("1", near.getAndRemove(1));

            assertNull(near.localPeek(1, CachePeekMode.ONHEAP));
            assertNull(dhtPeek(1));

            assertTrue(near.isLocalLocked(1, false));
            assertTrue(near.isLocalLocked(1, true));
        }
        finally {
            lock.unlock();
        }

        assertFalse(near.isLocalLocked(1, false));
        assertFalse(near.isLocalLocked(1, true));
    }

    /** @throws Exception If failed. */
    public void testSingleLockReentry() throws Exception {
        IgniteCache<Integer, String> near = jcache();

        Lock lock = near.lock(1);

        lock.lock();

        try {
            near.put(1, "1");

            assertEquals("1", near.localPeek(1, CachePeekMode.ONHEAP));
            assertEquals("1", dhtPeek(1));

            assertTrue(near.isLocalLocked(1, false));
            assertTrue(near.isLocalLocked(1, true));

            lock.lock(); // Reentry.

            try {
                assertEquals("1", near.get(1));
                assertEquals("1", near.getAndRemove(1));

                assertNull(near.localPeek(1, CachePeekMode.ONHEAP));
                assertNull(dhtPeek(1));

                assertTrue(near.isLocalLocked(1, false));
                assertTrue(near.isLocalLocked(1, true));
            }
            finally {
                lock.unlock();
            }

            assertTrue(near.isLocalLocked(1, false));
            assertTrue(near.isLocalLocked(1, true));
        }
        finally {
            lock.unlock();
        }

        assertFalse(near.isLocalLocked(1, false));
        assertFalse(near.isLocalLocked(1, true));
    }

    /** @throws Exception If failed. */
    public void testTransactionSingleGet() throws Exception {
        IgniteCache<Object, Object> cache = jcache();

        cache.put(1, "val1");

        assertEquals("val1", dhtPeek(1));
        assertNull(near().peekEx(1));

        Transaction tx = grid().transactions().txStart(PESSIMISTIC, REPEATABLE_READ);

        assertEquals("val1", cache.get(1));

        tx.commit();

        assertEquals("val1", dhtPeek(1));
        assertNull(near().peekEx(1));
    }

    /** @throws Exception If failed. */
    public void testTransactionSingleGetRemove() throws Exception {
        IgniteCache<Object, Object> cache = jcache();

        cache.put(1, "val1");

        assertEquals("val1", dhtPeek(1));
        assertNull(near().peekEx(1));

        Transaction tx = grid().transactions().txStart(PESSIMISTIC, REPEATABLE_READ);

        assertEquals("val1", cache.get(1));

        assertTrue(cache.remove(1));

        tx.commit();

        assertNull(dhtPeek(1));
        assertNull(near().peekEx(1));
    }

    /**
     *
     */
    private static class TestStore extends CacheStoreAdapter<Integer, String> {
        /** Map. */
        private ConcurrentMap<Integer, String> map = new ConcurrentHashMap<>();

        /** Create flag. */
        private volatile boolean create = true;

        /**
         *
         */
        void reset() {
            map.clear();

            create = true;
        }

        /** @return Create flag. */
        boolean isCreate() {
            return create;
        }

        /**
         * @param key Key.
         * @return Value.
         */
        String value(Integer key) {
            return map.get(key);
        }

        /**
         * @param key Key.
         * @return {@code True} if has value.
         */
        boolean hasValue(Integer key) {
            return map.containsKey(key);
        }

        /** @return {@code True} if empty. */
        boolean isEmpty() {
            return map.isEmpty();
        }

        /** {@inheritDoc} */
        @Override public String load(Integer key) {
            if (!create)
                return map.get(key);

            String s = map.putIfAbsent(key, key.toString());

            return s == null ? key.toString() : s;
        }

        /** {@inheritDoc} */
        @Override public void write(javax.cache.Cache.Entry<? extends Integer, ? extends String> e) {
            map.put(e.getKey(), e.getValue());
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            map.remove(key);
        }
    }
}