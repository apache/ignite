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
import com.google.common.collect.Sets;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.expiry.TouchedExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import junit.framework.AssertionFailedError;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.lang.GridAbsPredicateX;
import org.apache.ignite.internal.util.typedef.CIX1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CachePeekMode.ALL;
import static org.apache.ignite.cache.CachePeekMode.BACKUP;
import static org.apache.ignite.cache.CachePeekMode.OFFHEAP;
import static org.apache.ignite.cache.CachePeekMode.ONHEAP;
import static org.apache.ignite.cache.CachePeekMode.PRIMARY;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_LOCKED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_SWAPPED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_UNLOCKED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_UNSWAPPED;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;
import static org.apache.ignite.transactions.TransactionState.COMMITTED;

/**
 * Full API cache test.
 */
@SuppressWarnings({"TransientFieldInNonSerializableClass", "unchecked"})
public class CacheFullApiNewSelfTest extends CacheAbstractNewSelfTest {
    /** Test timeout */
    private static final long TEST_TIMEOUT = 60 * 1000;

    /** */
    public static final CacheEntryProcessor<String, Integer, String> ERR_PROCESSOR =
        new CacheEntryProcessor<String, Integer, String>() {
            /** */
            private static final long serialVersionUID = 0L;

            @Override public String process(MutableEntry<String, Integer> e, Object... args) {
                throw new RuntimeException("Failed!");
            }
        };

    /** Increment processor for invoke operations. */
    public static final EntryProcessor<TestObject, TestObject, TestObject> INCR_PROCESSOR = new IncrementEntryProcessor();

    /** Increment processor for invoke operations with IgniteEntryProcessor. */
    public static final CacheEntryProcessor<TestObject, TestObject, TestObject> INCR_IGNITE_PROCESSOR =
        new CacheEntryProcessor<TestObject, TestObject, TestObject>() {
            /** */
            private static final long serialVersionUID = 0L;

            @Override public TestObject process(MutableEntry<TestObject, TestObject> e, Object... args) {
                return INCR_PROCESSOR.process(e, args);
            }
        };

    /** Increment processor for invoke operations. */
    public static final EntryProcessor<TestObject, TestObject, TestObject> RMV_PROCESSOR = new RemoveEntryProcessor();

    /** Increment processor for invoke operations with IgniteEntryProcessor. */
    public static final CacheEntryProcessor<TestObject, TestObject, TestObject> RMV_IGNITE_PROCESSOR =
        new CacheEntryProcessor<TestObject, TestObject, TestObject>() {
            /** */
            private static final long serialVersionUID = 0L;

            @Override public TestObject process(MutableEntry<TestObject, TestObject> e, Object... args) {
                return RMV_PROCESSOR.process(e, args);
            }
        };

    /** Dflt grid. */
    protected transient Ignite dfltIgnite;

    /** */
    private Map<String, CacheConfiguration[]> cacheCfgMap;

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT;
    }

//    /** {@inheritDoc} */
//    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
//        IgniteConfiguration cfg = super.getConfiguration(gridName);
//
//        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);
//
//        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(true);
//
//        if (memoryMode() == OFFHEAP_TIERED || memoryMode() == OFFHEAP_VALUES)
//            cfg.setSwapSpaceSpi(new GridTestSwapSpaceSpi());
//
//        return cfg;
//    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        IgniteCache<String, Integer> cache = jcache();

        assertEquals(0, cache.localSize());
        assertEquals(0, cache.size());

        // TODO review.

        assertEquals(0, cache.localSize());
        assertEquals(0, cache.size());

        dfltIgnite = grid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        IgniteCache<String, Integer> cache = jcache();

        assertEquals(0, cache.localSize());
        assertEquals(0, cache.size());

        dfltIgnite = null;
    }

    /**
     * @return A not near-only cache.
     */
    protected IgniteCache<String, Integer> fullCache() {
        return jcache();
    }

    /**
     * @throws Exception In case of error.
     */
    public void testSize() throws Exception {
        assert jcache().localSize() == 0;

        int size = 10;

        final Map<String, Integer> map = new HashMap<>();

        for (int i = 0; i < size; i++)
            map.put("key" + i, i);

        // Put in primary nodes to avoid near readers which will prevent entry from being cleared.
        Map<ClusterNode, Collection<String>> mapped = grid(0).<String>affinity(cacheName()).mapKeysToNodes(map.keySet());

        for (int i = 0; i < gridCount(); i++) {
            Collection<String> keys = mapped.get(grid(i).localNode());

            if (!F.isEmpty(keys)) {
                for (String key : keys)
                    jcache(i).put(key, map.get(key));
            }
        }

        map.remove("key0");

        mapped = grid(0).<String>affinity(cacheName()).mapKeysToNodes(map.keySet());

        for (int i = 0; i < gridCount(); i++) {
            // Will actually delete entry from map.
            CU.invalidate(jcache(i), "key0");

            assertNull("Failed check for grid: " + i, jcache(i).localPeek("key0", ONHEAP));

            Collection<String> keysCol = mapped.get(grid(i).localNode());

            assert jcache(i).localSize() != 0 || F.isEmpty(keysCol);
        }

        for (int i = 0; i < gridCount(); i++)
            executeOnLocalOrRemoteJvm(i, new CheckCacheSizeTask(map, cacheName()));

        for (int i = 0; i < gridCount(); i++) {
            Collection<String> keysCol = mapped.get(grid(i).localNode());

            assertEquals("Failed check for grid: " + i, !F.isEmpty(keysCol) ? keysCol.size() : 0,
                jcache(i).localSize(PRIMARY));
        }

        int globalPrimarySize = map.size();

        for (int i = 0; i < gridCount(); i++)
            assertEquals(globalPrimarySize, jcache(i).size(PRIMARY));

        int times = 1;

        if (cacheMode() == REPLICATED)
            times = gridCount();
        else if (cacheMode() == PARTITIONED)
            times = Math.min(gridCount(), jcache().getConfiguration(CacheConfiguration.class).getBackups() + 1);

        int globalSize = globalPrimarySize * times;

        for (int i = 0; i < gridCount(); i++)
            assertEquals(globalSize, jcache(i).size(ALL));
    }

    /**
     * @throws Exception In case of error.
     */
    public void testContainsKey() throws Exception {
        jcache().put("testContainsKey", 1);

        checkContainsKey(true, "testContainsKey");
        checkContainsKey(false, "testContainsKeyWrongKey");
    }

    /**
     * @throws Exception If failed.
     */
    public void testContainsKeyTx() throws Exception {
        if (!txEnabled())
            return;

        IgniteCache<String, Integer> cache = jcache();

        IgniteTransactions txs = ignite(0).transactions();

        for (int i = 0; i < 10; i++) {
            String key = String.valueOf(i);

            try (Transaction tx = txs.txStart()) {
                assertNull(key, cache.get(key));

                assertFalse(cache.containsKey(key));

                tx.commit();
            }

            try (Transaction tx = txs.txStart()) {
                assertNull(key, cache.get(key));

                cache.put(key, i);

                assertTrue(cache.containsKey(key));

                tx.commit();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testContainsKeysTx() throws Exception {
        if (!txEnabled())
            return;

        IgniteCache<String, Integer> cache = jcache();

        IgniteTransactions txs = ignite(0).transactions();

        Set<String> keys = new HashSet<>();

        for (int i = 0; i < 10; i++) {
            String key = String.valueOf(i);

            keys.add(key);
        }

        try (Transaction tx = txs.txStart()) {
            for (String key : keys)
                assertNull(key, cache.get(key));

            assertFalse(cache.containsKeys(keys));

            tx.commit();
        }

        try (Transaction tx = txs.txStart()) {
            for (String key : keys)
                assertNull(key, cache.get(key));

            for (String key : keys)
                cache.put(key, 0);

            assertTrue(cache.containsKeys(keys));

            tx.commit();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveInExplicitLocks() throws Exception {
        if (lockingEnabled()) {
            IgniteCache<String, Integer> cache = jcache();

            cache.put("a", 1);

            Lock lock = cache.lockAll(ImmutableSet.of("a", "b", "c", "d"));

            lock.lock();

            try {
                cache.remove("a");

                // Make sure single-key operation did not remove lock.
                cache.putAll(F.asMap("b", 2, "c", 3, "d", 4));
            }
            finally {
                lock.unlock();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveAllSkipStore() throws Exception {
        if (isMultiJvm())
            fail("https://issues.apache.org/jira/browse/IGNITE-1088");

        if (!storeEnabled())
            return;

        IgniteCache<String, Integer> jcache = jcache();

        jcache.putAll(F.asMap("1", 1, "2", 2, "3", 3));

        jcache.withSkipStore().removeAll();

        assertEquals((Integer)1, jcache.get("1"));
        assertEquals((Integer)2, jcache.get("2"));
        assertEquals((Integer)3, jcache.get("3"));
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testAtomicOps() throws IgniteCheckedException {
        IgniteCache<String, Integer> c = jcache();

        final int cnt = 10;

        for (int i = 0; i < cnt; i++)
            assertNull(c.getAndPutIfAbsent("k" + i, i));

        for (int i = 0; i < cnt; i++) {
            boolean wrong = i % 2 == 0;

            String key = "k" + i;

            boolean res = c.replace(key, wrong ? i + 1 : i, -1);

            assertEquals(wrong, !res);
        }

        for (int i = 0; i < cnt; i++) {
            boolean success = i % 2 != 0;

            String key = "k" + i;

            boolean res = c.remove(key, -1);

            assertTrue(success == res);
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testGetSerializable() throws Exception {
        checkGet(DataMode.SERIALIZABLE);
    }

    /**
     * @throws Exception In case of error.
     */
    public void testGetExternalizable() throws Exception {
        checkGet(DataMode.EXTERNALIZABLE);
    }

    /**
     * @throws Exception In case of error.
     */
    public void testGetObject() throws Exception {
        checkGet(DataMode.PLANE_OBJECT);
    }

    /**
     * @param mode Mode.
     */
    private void checkGet(DataMode mode) {
        IgniteCache cache = jcache();

        cache.put(key(1, mode), value(1, mode));
        cache.put(key(2, mode), value(2, mode));

        assertEquals(value(1, mode), cache.get(key(1, mode)));
        assertEquals(value(2, mode), cache.get(key(2, mode)));
        // Wrong key.
        assertNull(cache.get(key(3, mode)));
    }

    /**
     * @throws Exception In case of error.
     */
    public void testGetAsync() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key1", 1);
        cache.put("key2", 2);

        IgniteCache<String, Integer> cacheAsync = cache.withAsync();

        cacheAsync.get("key1");

        IgniteFuture<Integer> fut1 = cacheAsync.future();

        cacheAsync.get("key2");

        IgniteFuture<Integer> fut2 = cacheAsync.future();

        cacheAsync.get("wrongKey");

        IgniteFuture<Integer> fut3 = cacheAsync.future();

        assert fut1.get() == 1;
        assert fut2.get() == 2;
        assert fut3.get() == null;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testGetAll() throws Exception {
        Transaction tx = txShouldBeUsed() ? transactions().txStart() : null;

        final IgniteCache<String, Integer> cache = jcache();

        try {
            cache.put("key1", 1);
            cache.put("key2", 2);

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.getAll(null).isEmpty();

                return null;
            }
        }, NullPointerException.class, null);

        assert cache.getAll(Collections.<String>emptySet()).isEmpty();

        Map<String, Integer> map1 = cache.getAll(ImmutableSet.of("key1", "key2", "key9999"));

        info("Retrieved map1: " + map1);

        assert 2 == map1.size() : "Invalid map: " + map1;

        assertEquals(1, (int)map1.get("key1"));
        assertEquals(2, (int)map1.get("key2"));
        assertNull(map1.get("key9999"));

        Map<String, Integer> map2 = cache.getAll(ImmutableSet.of("key1", "key2", "key9999"));

        info("Retrieved map2: " + map2);

        assert 2 == map2.size() : "Invalid map: " + map2;

        assertEquals(1, (int)map2.get("key1"));
        assertEquals(2, (int)map2.get("key2"));
        assertNull(map2.get("key9999"));

        // Now do the same checks but within transaction.
        if (txShouldBeUsed()) {
            try (Transaction tx0 = transactions().txStart()) {
                assert cache.getAll(Collections.<String>emptySet()).isEmpty();

                map1 = cache.getAll(ImmutableSet.of("key1", "key2", "key9999"));

                info("Retrieved map1: " + map1);

                assert 2 == map1.size() : "Invalid map: " + map1;

                assertEquals(1, (int)map1.get("key1"));
                assertEquals(2, (int)map1.get("key2"));
                assertNull(map1.get("key9999"));

                map2 = cache.getAll(ImmutableSet.of("key1", "key2", "key9999"));

                info("Retrieved map2: " + map2);

                assert 2 == map2.size() : "Invalid map: " + map2;

                assertEquals(1, (int)map2.get("key1"));
                assertEquals(2, (int)map2.get("key2"));
                assertNull(map2.get("key9999"));

                tx0.commit();
            }
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testGetAllWithNulls() throws Exception {
        final IgniteCache<String, Integer> cache = jcache();

        final Set<String> c = new HashSet<>();

        c.add("key1");
        c.add(null);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.getAll(c);

                return null;
            }
        }, NullPointerException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetTxNonExistingKey() throws Exception {
        if (txShouldBeUsed()) {
            try (Transaction ignored = transactions().txStart()) {
                assert jcache().get("key999123") == null;
            }
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testGetAllAsync() throws Exception {
        final IgniteCache<String, Integer> cache = jcache();

        final IgniteCache<String, Integer> cacheAsync = cache.withAsync();

        cache.put("key1", 1);
        cache.put("key2", 2);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cacheAsync.getAll(null);

                return null;
            }
        }, NullPointerException.class, null);

        cacheAsync.getAll(Collections.<String>emptySet());
        IgniteFuture<Map<String, Integer>> fut2 = cacheAsync.future();

        cacheAsync.getAll(ImmutableSet.of("key1", "key2"));
        IgniteFuture<Map<String, Integer>> fut3 = cacheAsync.future();

        assert fut2.get().isEmpty();
        assert fut3.get().size() == 2 : "Invalid map: " + fut3.get();
        assert fut3.get().get("key1") == 1;
        assert fut3.get().get("key2") == 2;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testPutSerializable() throws Exception {
        checkPut(DataMode.SERIALIZABLE);
    }

    /**
     * @throws Exception In case of error.
     */
    public void testPutExternalizable() throws Exception {
        checkPut(DataMode.EXTERNALIZABLE);
    }

    /**
     * @throws Exception In case of error.
     */
    public void testPutObject() throws Exception {
        checkPut(DataMode.PLANE_OBJECT);
    }

    /**
     * @param mode Mode.
     * @throws Exception If failed.
     */
    private void checkPut(DataMode mode) throws Exception {
        IgniteCache cache = jcache();

        final Object key1 = key(1, mode);
        final Object val1 = value(1, mode);
        final Object key2 = key(2, mode);
        final Object val2 = value(2, mode);

        assert cache.getAndPut(key1, val1) == null;
        assert cache.getAndPut(key2, val2) == null;

        // Check inside transaction.
        assertEquals(val1, cache.get(key1));
        assertEquals(val2, cache.get(key2));

        // Put again to check returned values.
        assertEquals(val1, cache.getAndPut(key1, val1));
        assertEquals(val2, cache.getAndPut(key2, val2));

        checkContainsKey(true, key1);
        checkContainsKey(true, key2);

        assert cache.get(key1) != null;
        assert cache.get(key2) != null;
        assert cache.get(key(100500, mode)) == null;

        // Check outside transaction.
        checkContainsKey(true, key1);
        checkContainsKey(true, key2);

        assertEquals(val1, cache.get(key1));
        assertEquals(val2, cache.get(key2));
        assert cache.get(key(100500, mode)) == null;

        assertEquals(val1, cache.getAndPut(key1, value(10, mode)));
        assertEquals(val2, cache.getAndPut(key2, value(11, mode)));
    }

    /**
     * @throws Exception In case of error.
     */
    public void testPutTx() throws Exception {
        if (txShouldBeUsed()) {
            IgniteCache<String, Integer> cache = jcache();

            try (Transaction tx = transactions().txStart()) {
                assert cache.getAndPut("key1", 1) == null;
                assert cache.getAndPut("key2", 2) == null;

                // Check inside transaction.
                assert cache.get("key1") == 1;
                assert cache.get("key2") == 2;

                // Put again to check returned values.
                assert cache.getAndPut("key1", 1) == 1;
                assert cache.getAndPut("key2", 2) == 2;

                assert cache.get("key1") != null;
                assert cache.get("key2") != null;
                assert cache.get("wrong") == null;

                tx.commit();
            }

            // Check outside transaction.
            checkContainsKey(true, "key1");
            checkContainsKey(true, "key2");

            assert cache.get("key1") == 1;
            assert cache.get("key2") == 2;
            assert cache.get("wrong") == null;

            assertEquals((Integer)1, cache.getAndPut("key1", 10));
            assertEquals((Integer)2, cache.getAndPut("key2", 11));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeOptimisticReadCommitted1() throws Exception {
        checkInvoke(OPTIMISTIC, READ_COMMITTED, DataMode.PLANE_OBJECT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeOptimisticReadCommitted2() throws Exception {
        checkInvoke(OPTIMISTIC, READ_COMMITTED, DataMode.SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeOptimisticReadCommitted3() throws Exception {
        checkInvoke(OPTIMISTIC, READ_COMMITTED, DataMode.EXTERNALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeOptimisticRepeatableRead1() throws Exception {
        checkInvoke(OPTIMISTIC, REPEATABLE_READ, DataMode.PLANE_OBJECT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeOptimisticRepeatableRead2() throws Exception {
        checkInvoke(OPTIMISTIC, REPEATABLE_READ, DataMode.SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeOptimisticRepeatableRead3() throws Exception {
        checkInvoke(OPTIMISTIC, REPEATABLE_READ, DataMode.PLANE_OBJECT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokePessimisticReadCommitted1() throws Exception {
        checkInvoke(PESSIMISTIC, READ_COMMITTED, DataMode.PLANE_OBJECT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokePessimisticReadCommitted2() throws Exception {
        checkInvoke(PESSIMISTIC, READ_COMMITTED, DataMode.SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokePessimisticReadCommitted3() throws Exception {
        checkInvoke(PESSIMISTIC, READ_COMMITTED, DataMode.EXTERNALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokePessimisticRepeatableRead1() throws Exception {
        checkInvoke(PESSIMISTIC, REPEATABLE_READ, DataMode.PLANE_OBJECT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokePessimisticRepeatableRead2() throws Exception {
        checkInvoke(PESSIMISTIC, REPEATABLE_READ, DataMode.SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokePessimisticRepeatableRead3() throws Exception {
        checkInvoke(PESSIMISTIC, REPEATABLE_READ, DataMode.EXTERNALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIgniteInvokeOptimisticReadCommitted1() throws Exception {
        checkIgniteInvoke(OPTIMISTIC, READ_COMMITTED, DataMode.PLANE_OBJECT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIgniteInvokeOptimisticReadCommitted2() throws Exception {
        checkIgniteInvoke(OPTIMISTIC, READ_COMMITTED, DataMode.SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIgniteInvokeOptimisticReadCommitted3() throws Exception {
        checkIgniteInvoke(OPTIMISTIC, READ_COMMITTED, DataMode.EXTERNALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIgniteInvokeOptimisticRepeatableRead1() throws Exception {
        checkIgniteInvoke(OPTIMISTIC, REPEATABLE_READ, DataMode.PLANE_OBJECT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIgniteInvokeOptimisticRepeatableRead2() throws Exception {
        checkIgniteInvoke(OPTIMISTIC, REPEATABLE_READ, DataMode.SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIgniteInvokeOptimisticRepeatableRead3() throws Exception {
        checkIgniteInvoke(OPTIMISTIC, REPEATABLE_READ, DataMode.EXTERNALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIgniteInvokePessimisticReadCommitted1() throws Exception {
        checkIgniteInvoke(PESSIMISTIC, READ_COMMITTED, DataMode.PLANE_OBJECT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIgniteInvokePessimisticReadCommitted2() throws Exception {
        checkIgniteInvoke(PESSIMISTIC, READ_COMMITTED, DataMode.SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIgniteInvokePessimisticReadCommitted3() throws Exception {
        checkIgniteInvoke(PESSIMISTIC, READ_COMMITTED, DataMode.EXTERNALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIgniteInvokePessimisticRepeatableRead1() throws Exception {
        checkIgniteInvoke(PESSIMISTIC, REPEATABLE_READ, DataMode.PLANE_OBJECT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIgniteInvokePessimisticRepeatableRead2() throws Exception {
        checkIgniteInvoke(PESSIMISTIC, REPEATABLE_READ, DataMode.SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIgniteInvokePessimisticRepeatableRead3() throws Exception {
        checkIgniteInvoke(PESSIMISTIC, REPEATABLE_READ, DataMode.EXTERNALIZABLE);
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws Exception If failed.
     */
    private void checkIgniteInvoke(TransactionConcurrency concurrency, TransactionIsolation isolation, DataMode mode)
        throws Exception {
        checkInvoke(concurrency, isolation, mode, INCR_IGNITE_PROCESSOR, RMV_IGNITE_PROCESSOR);
    }

    /**
     * @param concurrency Transaction concurrency.
     * @param isolation Transaction isolation.
     * @param mode Data mode.
     * @param incrProcessor Increment processor.
     * @param rmvProseccor Remove processor.
     */
    private void checkInvoke(TransactionConcurrency concurrency, TransactionIsolation isolation,
        DataMode mode,
        EntryProcessor<TestObject, TestObject, TestObject> incrProcessor,
        EntryProcessor<TestObject, TestObject, TestObject> rmvProseccor) {
        IgniteCache cache = jcache();

        final Object key1 = key(1, mode);
        final Object key2 = key(2, mode);
        final Object key3 = key(3, mode);

        final Object val1 = value(1, mode);
        final Object val2 = value(2, mode);
        final Object val3 = value(3, mode);

        cache.put(key2, val1);
        cache.put(key3, val3);

        Transaction tx = txShouldBeUsed() ? ignite(0).transactions().txStart(concurrency, isolation) : null;

        try {
            assertNull(cache.invoke(key1, incrProcessor, mode));
            assertEquals(val1, cache.invoke(key2, incrProcessor, mode));
            assertEquals(val3, cache.invoke(key3, rmvProseccor));

            if (tx != null)
                tx.commit();
        }
        catch (Exception e) {
            e.printStackTrace();

            throw e;
        }
        finally {
            if (tx != null)
                tx.close();
        }

        assertEquals(val1, cache.get(key1));
        assertEquals(val2, cache.get(key2));
        assertNull(cache.get(key3));

        for (int i = 0; i < gridCount(); i++)
            assertNull("Failed for cache: " + i, jcache(i).localPeek(key3, ONHEAP));

        cache.remove(key1);
        cache.put(key2, val1);
        cache.put(key3, val3);

        assertNull(cache.invoke(key1, incrProcessor, mode));
        assertEquals(val1, cache.invoke(key2, incrProcessor, mode));
        assertEquals(val3, cache.invoke(key3, rmvProseccor));

        assertEquals(val1, cache.get(key1));
        assertEquals(val2, cache.get(key2));
        assertNull(cache.get(key3));

        for (int i = 0; i < gridCount(); i++)
            assertNull(jcache(i).localPeek(key3, ONHEAP));
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @param mode
     * @throws Exception If failed.
     */
    private void checkInvoke(TransactionConcurrency concurrency, TransactionIsolation isolation,
        DataMode mode) throws Exception {
        checkInvoke(concurrency, isolation, mode, INCR_PROCESSOR, RMV_PROCESSOR);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeAllOptimisticReadCommitted1() throws Exception {
        checkInvokeAll(OPTIMISTIC, READ_COMMITTED, DataMode.PLANE_OBJECT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeAllOptimisticReadCommitted2() throws Exception {
        checkInvokeAll(OPTIMISTIC, READ_COMMITTED, DataMode.SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeAllOptimisticReadCommitted3() throws Exception {
        checkInvokeAll(OPTIMISTIC, READ_COMMITTED, DataMode.EXTERNALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeAllOptimisticRepeatableRead1() throws Exception {
        checkInvokeAll(OPTIMISTIC, REPEATABLE_READ, DataMode.PLANE_OBJECT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeAllOptimisticRepeatableRead2() throws Exception {
        checkInvokeAll(OPTIMISTIC, REPEATABLE_READ, DataMode.SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeAllOptimisticRepeatableRead3() throws Exception {
        checkInvokeAll(OPTIMISTIC, REPEATABLE_READ, DataMode.EXTERNALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeAllPessimisticReadCommitted1() throws Exception {
        checkInvokeAll(PESSIMISTIC, READ_COMMITTED, DataMode.PLANE_OBJECT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeAllPessimisticReadCommitted2() throws Exception {
        checkInvokeAll(PESSIMISTIC, READ_COMMITTED, DataMode.SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeAllPessimisticReadCommitted3() throws Exception {
        checkInvokeAll(PESSIMISTIC, READ_COMMITTED, DataMode.EXTERNALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeAllPessimisticRepeatableRead1() throws Exception {
        checkInvokeAll(PESSIMISTIC, REPEATABLE_READ, DataMode.PLANE_OBJECT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeAllPessimisticRepeatableRead2() throws Exception {
        checkInvokeAll(PESSIMISTIC, REPEATABLE_READ, DataMode.SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeAllPessimisticRepeatableRead3() throws Exception {
        checkInvokeAll(PESSIMISTIC, REPEATABLE_READ, DataMode.EXTERNALIZABLE);
    }

    /**
     * @param concurrency Transaction concurrency.
     * @param isolation Transaction isolation.
     * @param mode Data mode.
     * @throws Exception If failed.
     */
    private void checkInvokeAll(TransactionConcurrency concurrency, TransactionIsolation isolation,
        DataMode mode)
        throws Exception {
        final TestObject key1 = key(1, mode);
        final TestObject key2 = key(2, mode);
        final TestObject key3 = key(3, mode);

        final TestObject val1 = value(1, mode);
        final TestObject val2 = value(2, mode);
        final TestObject val3 = value(3, mode);
        final TestObject val4 = value(4, mode);

        final IgniteCache<TestObject, TestObject> cache = jcache();

        cache.put(key2, val1);
        cache.put(key3, val3);

        if (txShouldBeUsed()) {
            Map<TestObject, EntryProcessorResult<TestObject>> res;

            try (Transaction tx = ignite(0).transactions().txStart(concurrency, isolation)) {
                res = cache.invokeAll(F.asSet(key1, key2, key3), INCR_PROCESSOR, mode);

                tx.commit();
            }

            assertEquals(val1, cache.get(key1));
            assertEquals(val2, cache.get(key2));
            assertEquals(val4, cache.get(key3));

            assertNull(res.get(key1));
            assertEquals(val1, res.get(key2).get());
            assertEquals(val3, res.get(key3).get());

            assertEquals(2, res.size());

            cache.remove(key1);
            cache.put(key2, val1);
            cache.put(key3, val3);
        }

        Map<TestObject, EntryProcessorResult<TestObject>> res = cache.invokeAll(F.asSet(key1, key2, key3), RMV_PROCESSOR);

        for (int i = 0; i < gridCount(); i++) {
            assertNull(jcache(i).localPeek(key1, ONHEAP));
            assertNull(jcache(i).localPeek(key2, ONHEAP));
            assertNull(jcache(i).localPeek(key3, ONHEAP));
        }

        assertNull(res.get(key1));
        assertEquals(val1, res.get(key2).get());
        assertEquals(val3, res.get(key3).get());

        assertEquals(2, res.size());

        cache.remove(key1);
        cache.put(key2, val1);
        cache.put(key3, val3);

        res = cache.invokeAll(F.asSet(key1, key2, key3), INCR_PROCESSOR, mode);

        assertEquals(val1, cache.get(key1));
        assertEquals(val2, cache.get(key2));
        assertEquals(val4, cache.get(key3));

        assertNull(res.get(key1));
        assertEquals(val1, res.get(key2).get());
        assertEquals(val3, res.get(key3).get());

        assertEquals(2, res.size());

        cache.remove(key1);
        cache.put(key2, val1);
        cache.put(key3, val3);

        res = cache.invokeAll(F.asMap(key1, INCR_PROCESSOR, key2, INCR_PROCESSOR, key3, INCR_PROCESSOR), mode);

        assertEquals(val1, cache.get(key1));
        assertEquals(val2, cache.get(key2));
        assertEquals(val4, cache.get(key3));

        assertNull(res.get(key1));
        assertEquals(val1, res.get(key2).get());
        assertEquals(val3, res.get(key3).get());

        assertEquals(2, res.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeAllWithNulls1() throws Exception {
        checkInvokeAllWithNulls(DataMode.PLANE_OBJECT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeAllWithNulls2() throws Exception {
        checkInvokeAllWithNulls(DataMode.SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeAllWithNulls3() throws Exception {
        checkInvokeAllWithNulls(DataMode.EXTERNALIZABLE);
    }

    /**
     * @param mode Mode.
     */
    private void checkInvokeAllWithNulls(final DataMode mode) {
        final TestObject key1 = key(1, mode);

        final IgniteCache<TestObject, TestObject> cache = jcache();

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.invokeAll((Set<TestObject>)null, INCR_PROCESSOR, mode);

                return null;
            }
        }, NullPointerException.class, null);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.invokeAll(F.asSet(key1), null);

                return null;
            }
        }, NullPointerException.class, null);

        {
            final Set<TestObject> keys = new LinkedHashSet<>(2);

            keys.add(key1);
            keys.add(null);

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    cache.invokeAll(keys, INCR_PROCESSOR, mode);

                    return null;
                }
            }, NullPointerException.class, null);

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    cache.invokeAll(F.asSet(key1), null);

                    return null;
                }
            }, NullPointerException.class, null);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeSequentialOptimisticNoStart1() throws Exception {
        checkInvokeSequential0(false, OPTIMISTIC, DataMode.PLANE_OBJECT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeSequentialOptimisticNoStart2() throws Exception {
        checkInvokeSequential0(false, OPTIMISTIC, DataMode.SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeSequentialOptimisticNoStart3() throws Exception {
        checkInvokeSequential0(false, OPTIMISTIC, DataMode.EXTERNALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeSequentialPessimisticNoStart1() throws Exception {
        checkInvokeSequential0(false, PESSIMISTIC, DataMode.PLANE_OBJECT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeSequentialPessimisticNoStart2() throws Exception {
        checkInvokeSequential0(false, PESSIMISTIC, DataMode.SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeSequentialPessimisticNoStart3() throws Exception {
        checkInvokeSequential0(false, PESSIMISTIC, DataMode.EXTERNALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeSequentialOptimisticWithStart1() throws Exception {
        checkInvokeSequential0(true, OPTIMISTIC, DataMode.PLANE_OBJECT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeSequentialOptimisticWithStart2() throws Exception {
        checkInvokeSequential0(true, OPTIMISTIC, DataMode.SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeSequentialOptimisticWithStart3() throws Exception {
        checkInvokeSequential0(true, OPTIMISTIC, DataMode.EXTERNALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeSequentialPessimisticWithStart1() throws Exception {
        checkInvokeSequential0(true, PESSIMISTIC, DataMode.PLANE_OBJECT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeSequentialPessimisticWithStart2() throws Exception {
        checkInvokeSequential0(true, PESSIMISTIC, DataMode.SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeSequentialPessimisticWithStart3() throws Exception {
        checkInvokeSequential0(true, PESSIMISTIC, DataMode.EXTERNALIZABLE);
    }

    /**
     * @param startVal Whether to put value.
     * @param concurrency Concurrency.
     * @param mode Data mode.
     * @throws Exception If failed.
     */
    private void checkInvokeSequential0(boolean startVal, TransactionConcurrency concurrency,
        DataMode mode)
        throws Exception {
        final TestObject val1 = value(1, mode);
        final TestObject val2 = value(2, mode);
        final TestObject val3 = value(3, mode);

        IgniteCache<TestObject, TestObject> cache = jcache();

        final TestObject key = primaryTestObjectKeysForCache(cache, 1, mode).get(0);

        Transaction tx = txShouldBeUsed() ? ignite(0).transactions().txStart(concurrency, READ_COMMITTED) : null;

        try {
            if (startVal)
                cache.put(key, val2);
            else
                assertEquals(null, cache.get(key));

            TestObject expRes = startVal ? val2 : null;

            assertEquals(expRes, cache.invoke(key, INCR_PROCESSOR, mode));

            expRes = startVal ? val3 : val1;

            assertEquals(expRes, cache.invoke(key, INCR_PROCESSOR, mode));

            expRes = value(expRes.value() + 1, mode);

            assertEquals(expRes, cache.invoke(key, INCR_PROCESSOR, mode));

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        TestObject exp = value((startVal ? 2 : 0) + 3, mode);

        assertEquals(exp, cache.get(key));

        for (int i = 0; i < gridCount(); i++) {
            if (ignite(i).affinity(cacheName()).isPrimaryOrBackup(grid(i).localNode(), key))
                assertEquals(exp, peek(jcache(i), key));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeAfterRemoveOptimistic1() throws Exception {
        checkInvokeAfterRemove(OPTIMISTIC, DataMode.PLANE_OBJECT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeAfterRemoveOptimistic2() throws Exception {
        checkInvokeAfterRemove(OPTIMISTIC, DataMode.SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeAfterRemoveOptimistic3() throws Exception {
        checkInvokeAfterRemove(OPTIMISTIC, DataMode.EXTERNALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeAfterRemovePessimistic1() throws Exception {
        checkInvokeAfterRemove(PESSIMISTIC, DataMode.PLANE_OBJECT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeAfterRemovePessimistic2() throws Exception {
        checkInvokeAfterRemove(PESSIMISTIC, DataMode.SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeAfterRemovePessimistic3() throws Exception {
        checkInvokeAfterRemove(PESSIMISTIC, DataMode.EXTERNALIZABLE);
    }

    /**
     * @param concurrency Concurrency.
     * @param mode Mode.
     * @throws Exception If failed.
     */
    private void checkInvokeAfterRemove(TransactionConcurrency concurrency, DataMode mode) throws Exception {
        IgniteCache<TestObject, TestObject> cache = jcache();

        TestObject key = key(1, mode);

        cache.put(key, value(4, mode));

        Transaction tx = txShouldBeUsed() ? ignite(0).transactions().txStart(concurrency, READ_COMMITTED) : null;

        try {
            cache.remove(key);

            cache.invoke(key, INCR_PROCESSOR, mode);
            cache.invoke(key, INCR_PROCESSOR, mode);
            cache.invoke(key, INCR_PROCESSOR, mode);

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        assertEquals(value(3, mode), cache.get(key));
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeReturnValueGetOptimisticReadCommitted1() throws Exception {
        checkInvokeReturnValue(false, OPTIMISTIC, READ_COMMITTED, DataMode.PLANE_OBJECT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeReturnValueGetOptimisticReadCommitted2() throws Exception {
        checkInvokeReturnValue(false, OPTIMISTIC, READ_COMMITTED, DataMode.SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeReturnValueGetOptimisticReadCommitted3() throws Exception {
        checkInvokeReturnValue(false, OPTIMISTIC, READ_COMMITTED, DataMode.EXTERNALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testInvokeReturnValueGetOptimisticRepeatableRead1() throws Exception {
        checkInvokeReturnValue(false, OPTIMISTIC, REPEATABLE_READ, DataMode.PLANE_OBJECT);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testInvokeReturnValueGetOptimisticRepeatableRead2() throws Exception {
        checkInvokeReturnValue(false, OPTIMISTIC, REPEATABLE_READ, DataMode.SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testInvokeReturnValueGetOptimisticRepeatableRead3() throws Exception {
        checkInvokeReturnValue(false, OPTIMISTIC, REPEATABLE_READ, DataMode.EXTERNALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testInvokeReturnValueGetPessimisticReadCommitted1() throws Exception {
        checkInvokeReturnValue(false, PESSIMISTIC, READ_COMMITTED, DataMode.PLANE_OBJECT);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testInvokeReturnValueGetPessimisticReadCommitted2() throws Exception {
        checkInvokeReturnValue(false, PESSIMISTIC, READ_COMMITTED, DataMode.SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testInvokeReturnValueGetPessimisticReadCommitted3() throws Exception {
        checkInvokeReturnValue(false, PESSIMISTIC, READ_COMMITTED, DataMode.EXTERNALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testInvokeReturnValueGetPessimisticRepeatableRead1() throws Exception {
        checkInvokeReturnValue(false, PESSIMISTIC, REPEATABLE_READ, DataMode.PLANE_OBJECT);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testInvokeReturnValueGetPessimisticRepeatableRead2() throws Exception {
        checkInvokeReturnValue(false, PESSIMISTIC, REPEATABLE_READ, DataMode.SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testInvokeReturnValueGetPessimisticRepeatableRead3() throws Exception {
        checkInvokeReturnValue(false, PESSIMISTIC, REPEATABLE_READ, DataMode.EXTERNALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testInvokeReturnValuePutInTx1() throws Exception {
        checkInvokeReturnValue(true, OPTIMISTIC, READ_COMMITTED, DataMode.PLANE_OBJECT);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testInvokeReturnValuePutInTx2() throws Exception {
        checkInvokeReturnValue(true, OPTIMISTIC, READ_COMMITTED, DataMode.SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testInvokeReturnValuePutInTx3() throws Exception {
        checkInvokeReturnValue(true, OPTIMISTIC, READ_COMMITTED, DataMode.EXTERNALIZABLE);
    }

    /**
     * @param put Whether to put value.
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws Exception If failed.
     */
    private void checkInvokeReturnValue(boolean put,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation,
        DataMode mode)
        throws Exception
    {
        IgniteCache<TestObject, TestObject> cache = jcache();

        TestObject key = key(1, mode);
        TestObject val1 = value(1, mode);
        TestObject val2 = value(2, mode);

        if (!put)
            cache.put(key, val1);

        Transaction tx = txShouldBeUsed() ? ignite(0).transactions().txStart(concurrency, isolation) : null;

        try {
            if (put)
                cache.put(key, val1);

            cache.invoke(key, INCR_PROCESSOR, mode);

            assertEquals(val2, cache.get(key));

            if (tx != null) {
                // Second get inside tx. Make sure read value is not transformed twice.
                assertEquals(val2, cache.get(key));

                tx.commit();
            }
        }
        finally {
            if (tx != null)
                tx.close();
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void _testGetAndPutAsync() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        IgniteCache<String, Integer> cacheAsync = cache.withAsync();

        cache.put("key1", 1);
        cache.put("key2", 2);

        cacheAsync.getAndPut("key1", 10);

        IgniteFuture<Integer> fut1 = cacheAsync.future();

        cacheAsync.getAndPut("key2", 11);

        IgniteFuture<Integer> fut2 = cacheAsync.future();

        assertEquals((Integer)1, fut1.get(5000));
        assertEquals((Integer)2, fut2.get(5000));

        assertEquals((Integer)10, cache.get("key1"));
        assertEquals((Integer)11, cache.get("key2"));
    }

    /**
     * @throws Exception In case of error.
     */
    public void _testPutAsync0() throws Exception {
        IgniteCache cacheAsync = jcache().withAsync();

        cacheAsync.getAndPut("key1", 0);

        IgniteFuture<Integer> fut1 = cacheAsync.future();

        cacheAsync.getAndPut("key2", 1);

        IgniteFuture<Integer> fut2 = cacheAsync.future();

        assert fut1.get(5000) == null;
        assert fut2.get(5000) == null;
    }

    /**
     * @throws Exception If failed.
     */
    public void _testInvokeAsync1() throws Exception {
        checkInvokeAsync(DataMode.PLANE_OBJECT);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testInvokeAsync2() throws Exception {
        checkInvokeAsync(DataMode.SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testInvokeAsync3() throws Exception {
        checkInvokeAsync(DataMode.EXTERNALIZABLE);
    }

    /**
     * @param mode Mode.
     */
    private void checkInvokeAsync(DataMode mode) {
        final TestObject key1 = key(1, mode);
        final TestObject key2 = key(2, mode);
        final TestObject key3 = key(3, mode);

        final TestObject val1 = value(1, mode);
        final TestObject val2 = value(2, mode);
        final TestObject val3 = value(3, mode);

        IgniteCache<TestObject, TestObject> cache = jcache();

        cache.put(key2, val1);
        cache.put(key3, val3);

        IgniteCache<TestObject, TestObject> cacheAsync = cache.withAsync();

        assertNull(cacheAsync.invoke(key1, INCR_PROCESSOR, mode));

        IgniteFuture<?> fut0 = cacheAsync.future();

        assertNull(cacheAsync.invoke(key2, INCR_PROCESSOR, mode));

        IgniteFuture<?> fut1 = cacheAsync.future();

        assertNull(cacheAsync.invoke(key3, RMV_PROCESSOR));

        IgniteFuture<?> fut2 = cacheAsync.future();

        fut0.get();
        fut1.get();
        fut2.get();

        assertEquals(val1, cache.get(key1));
        assertEquals(val2, cache.get(key2));
        assertNull(cache.get(key3));

        for (int i = 0; i < gridCount(); i++)
            assertNull(jcache(i).localPeek(key3, ONHEAP));
    }

    /**
     * @throws Exception If failed.
     */
    public void _testInvoke1() throws Exception {
        checkInvoke(DataMode.PLANE_OBJECT);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testInvoke2() throws Exception {
        checkInvoke(DataMode.SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testInvoke3() throws Exception {
        checkInvoke(DataMode.EXTERNALIZABLE);
    }

    /**
     * @param mode Mode.
     */
    private void checkInvoke(DataMode mode) {
        final TestObject k0 = key(0, mode);
        final TestObject k1 = key(1, mode);

        final TestObject val1 = value(1, mode);
        final TestObject val2 = value(2, mode);
        final TestObject val3 = value(3, mode);

        final IgniteCache<TestObject, TestObject> cache = jcache();

        assertNull(cache.invoke(k0, INCR_PROCESSOR, mode));

        assertEquals(k1, cache.get(k0));

        assertEquals(val1, cache.invoke(k0, INCR_PROCESSOR, mode));

        assertEquals(val2, cache.get(k0));

        cache.put(k1, val1);

        assertEquals(val1, cache.invoke(k1, INCR_PROCESSOR, mode));

        assertEquals(val2, cache.get(k1));

        assertEquals(val2, cache.invoke(k1, INCR_PROCESSOR, mode));

        assertEquals(val3, cache.get(k1));

        RemoveAndReturnNullEntryProcessor c = new RemoveAndReturnNullEntryProcessor();

        assertNull(cache.invoke(k1, c));
        assertNull(cache.get(k1));

        for (int i = 0; i < gridCount(); i++)
            assertNull(jcache(i).localPeek(k1, ONHEAP));

        final EntryProcessor<TestObject, TestObject, TestObject> errProcessor = new FailedEntryProcessor();

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.invoke(k1, errProcessor);

                return null;
            }
        }, EntryProcessorException.class, "Test entry processor exception.");
    }

    /**
     * @throws Exception In case of error.
     */
    public void _testPutx() throws Exception {
        if (txShouldBeUsed())
            checkPut(true);
    }

    /**
     * @throws Exception In case of error.
     */
    public void _testPutxNoTx() throws Exception {
        checkPut(false);
    }

    /**
     * @param inTx Whether to start transaction.
     * @throws Exception If failed.
     */
    private void checkPut(boolean inTx) throws Exception {
        Transaction tx = inTx ? transactions().txStart() : null;

        IgniteCache<String, Integer> cache = jcache();

        try {
            cache.put("key1", 1);
            cache.put("key2", 2);

            // Check inside transaction.
            assert cache.get("key1") == 1;
            assert cache.get("key2") == 2;

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        checkSize(F.asSet("key1", "key2"));

        // Check outside transaction.
        checkContainsKey(true, "key1");
        checkContainsKey(true, "key2");
        checkContainsKey(false, "wrong");

        assert cache.get("key1") == 1;
        assert cache.get("key2") == 2;
        assert cache.get("wrong") == null;
    }

    /**
     * @throws Exception If failed.
     */
    public void _testPutAsync() throws Exception {
        Transaction tx = txShouldBeUsed() ? transactions().txStart() : null;

        IgniteCache cacheAsync = jcache().withAsync();

        try {
            jcache().put("key2", 1);

            cacheAsync.put("key1", 10);

            IgniteFuture<?> fut1 = cacheAsync.future();

            cacheAsync.put("key2", 11);

            IgniteFuture<?> fut2 = cacheAsync.future();

            IgniteFuture<Transaction> f = null;

            if (tx != null) {
                tx = (Transaction)tx.withAsync();

                tx.commit();

                f = tx.future();
            }

            assertNull(fut1.get());
            assertNull(fut2.get());

            assert f == null || f.get().state() == COMMITTED;
        }
        finally {
            if (tx != null)
                tx.close();
        }

        checkSize(F.asSet("key1", "key2"));

        assert jcache().get("key1") == 10;
        assert jcache().get("key2") == 11;
    }

    /**
     * @throws Exception In case of error.
     */
    public void _testPutAll() throws Exception {
        Map<String, Integer> map = F.asMap("key1", 1, "key2", 2);

        IgniteCache<String, Integer> cache = jcache();

        cache.putAll(map);

        checkSize(F.asSet("key1", "key2"));

        assert cache.get("key1") == 1;
        assert cache.get("key2") == 2;

        map.put("key1", 10);
        map.put("key2", 20);

        cache.putAll(map);

        checkSize(F.asSet("key1", "key2"));

        assert cache.get("key1") == 10;
        assert cache.get("key2") == 20;
    }

    /**
     * @throws Exception In case of error.
     */
    public void _testNullInTx() throws Exception {
        if (!txShouldBeUsed())
            return;

        final IgniteCache<String, Integer> cache = jcache();

        for (int i = 0; i < 100; i++) {
            final String key = "key-" + i;

            assertNull(cache.get(key));

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    IgniteTransactions txs = transactions();

                    try (Transaction tx = txs.txStart()) {
                        cache.put(key, 1);

                        cache.put(null, 2);

                        tx.commit();
                    }

                    return null;
                }
            }, NullPointerException.class, null);

            assertNull(cache.get(key));

            cache.put(key, 1);

            assertEquals(1, (int) cache.get(key));

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    IgniteTransactions txs = transactions();

                    try (Transaction tx = txs.txStart()) {
                        cache.put(key, 2);

                        cache.remove(null);

                        tx.commit();
                    }

                    return null;
                }
            }, NullPointerException.class, null);

            assertEquals(1, (int) cache.get(key));

            cache.put(key, 2);

            assertEquals(2, (int)cache.get(key));

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    IgniteTransactions txs = transactions();

                    Map<String, Integer> map = new LinkedHashMap<>();

                    map.put("k1", 1);
                    map.put("k2", 2);
                    map.put(null, 3);

                    try (Transaction tx = txs.txStart()) {
                        cache.put(key, 1);

                        cache.putAll(map);

                        tx.commit();
                    }

                    return null;
                }
            }, NullPointerException.class, null);

            assertNull(cache.get("k1"));
            assertNull(cache.get("k2"));

            assertEquals(2, (int) cache.get(key));

            cache.put(key, 3);

            assertEquals(3, (int)cache.get(key));
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void _testPutAllWithNulls() throws Exception {
        final IgniteCache<String, Integer> cache = jcache();

        {
            final Map<String, Integer> m = new LinkedHashMap<>(2);

            m.put("key1", 1);
            m.put(null, 2);

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    cache.putAll(m);

                    return null;
                }
            }, NullPointerException.class, null);

            cache.put("key1", 1);

            assertEquals(1, (int)cache.get("key1"));
        }

        {
            final Map<String, Integer> m = new LinkedHashMap<>(2);

            m.put("key3", 3);
            m.put("key4", null);

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    cache.putAll(m);

                    return null;
                }
            }, NullPointerException.class, null);

            m.put("key4", 4);

            cache.putAll(m);

            assertEquals(3, (int) cache.get("key3"));
            assertEquals(4, (int)cache.get("key4"));
        }

        assertThrows(log, new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                cache.put("key1", null);

                return null;
            }
        }, NullPointerException.class, A.NULL_MSG_PREFIX);

        assertThrows(log, new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                cache.getAndPut("key1", null);

                return null;
            }
        }, NullPointerException.class, A.NULL_MSG_PREFIX);

        assertThrows(log, new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                cache.put(null, 1);

                return null;
            }
        }, NullPointerException.class, A.NULL_MSG_PREFIX);

        assertThrows(log, new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                cache.replace(null, 1);

                return null;
            }
        }, NullPointerException.class, A.NULL_MSG_PREFIX);

        assertThrows(log, new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                cache.getAndReplace(null, 1);

                return null;
            }
        }, NullPointerException.class, A.NULL_MSG_PREFIX);

        assertThrows(log, new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                cache.replace("key", null);

                return null;
            }
        }, NullPointerException.class, A.NULL_MSG_PREFIX);

        assertThrows(log, new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                cache.getAndReplace("key", null);

                return null;
            }
        }, NullPointerException.class, A.NULL_MSG_PREFIX);

        assertThrows(log, new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                cache.replace(null, 1, 2);

                return null;
            }
        }, NullPointerException.class, A.NULL_MSG_PREFIX);

        assertThrows(log, new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                cache.replace("key", null, 2);

                return null;
            }
        }, NullPointerException.class, A.NULL_MSG_PREFIX);

        assertThrows(log, new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                cache.replace("key", 1, null);

                return null;
            }
        }, NullPointerException.class, A.NULL_MSG_PREFIX);
    }

    /**
     * @throws Exception In case of error.
     */
    public void _testPutAllAsync() throws Exception {
        Map<String, Integer> map = F.asMap("key1", 1, "key2", 2);

        IgniteCache<String, Integer> cache = jcache();

        IgniteCache<String, Integer> cacheAsync = cache.withAsync();

        cacheAsync.putAll(map);

        IgniteFuture<?> f1 = cacheAsync.future();

        map.put("key1", 10);
        map.put("key2", 20);

        cacheAsync.putAll(map);

        IgniteFuture<?> f2 = cacheAsync.future();

        assertNull(f2.get());
        assertNull(f1.get());

        checkSize(F.asSet("key1", "key2"));

        assert cache.get("key1") == 10;
        assert cache.get("key2") == 20;
    }

    /**
     * @throws Exception In case of error.
     */
    public void _testGetAndPutIfAbsent() throws Exception {
        Transaction tx = txShouldBeUsed() ? transactions().txStart() : null;

        IgniteCache<String, Integer> cache = jcache();

        try {
            assert cache.getAndPutIfAbsent("key", 1) == null;

            assert cache.get("key") != null;
            assert cache.get("key") == 1;

            assert cache.getAndPutIfAbsent("key", 2) != null;
            assert cache.getAndPutIfAbsent("key", 2) == 1;

            assert cache.get("key") != null;
            assert cache.get("key") == 1;

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        assert cache.getAndPutIfAbsent("key", 2) != null;

        for (int i = 0; i < gridCount(); i++) {
            info("Peek on node [i=" + i + ", id=" + grid(i).localNode().id() + ", val=" +
                grid(i).cache(cacheName()).localPeek("key", ONHEAP) + ']');
        }

        assertEquals((Integer)1, cache.getAndPutIfAbsent("key", 2));

        assert cache.get("key") != null;
        assert cache.get("key") == 1;

        if (!storeEnabled())
            return;

        // Check swap.
        cache.put("key2", 1);

        cache.localEvict(Collections.singleton("key2"));

        if (!isLoadPreviousValue())
            cache.get("key2");

        assertEquals((Integer)1, cache.getAndPutIfAbsent("key2", 3));

        // Check db.
        if (storeEnabled() && isLoadPreviousValue() && !isMultiJvm()) {
            putToStore("key3", 3);

            assertEquals((Integer)3, cache.getAndPutIfAbsent("key3", 4));

            assertEquals((Integer)3, cache.get("key3"));
        }

        assertEquals((Integer)1, cache.get("key2"));

        cache.localEvict(Collections.singleton("key2"));

        if (!isLoadPreviousValue())
            cache.get("key2");

        // Same checks inside tx.
        tx = txShouldBeUsed() ? transactions().txStart() : null;

        try {
            assertEquals((Integer)1, cache.getAndPutIfAbsent("key2", 3));

            if (tx != null)
                tx.commit();

            assertEquals((Integer)1, cache.get("key2"));
        }
        finally {
            if (tx != null)
                tx.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void _testGetAndPutIfAbsentAsync() throws Exception {
        Transaction tx = txShouldBeUsed() ? transactions().txStart() : null;

        IgniteCache<String, Integer> cache = jcache();

        IgniteCache<String, Integer> cacheAsync = cache.withAsync();

        try {
            cacheAsync.getAndPutIfAbsent("key", 1);

            IgniteFuture<Integer> fut1 = cacheAsync.future();

            assertNull(fut1.get());
            assertEquals((Integer)1, cache.get("key"));

            cacheAsync.getAndPutIfAbsent("key", 2);

            IgniteFuture<Integer> fut2 = cacheAsync.future();

            assertEquals((Integer)1, fut2.get());
            assertEquals((Integer)1, cache.get("key"));

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        if (!storeEnabled())
            return;

        // Check swap.
        cache.put("key2", 1);

        cache.localEvict(Collections.singleton("key2"));

        if (!isLoadPreviousValue())
            cache.get("key2");

        cacheAsync.getAndPutIfAbsent("key2", 3);

        assertEquals((Integer)1, cacheAsync.<Integer>future().get());

        // Check db.
        if (storeEnabled() && isLoadPreviousValue() && !isMultiJvm()) {
            putToStore("key3", 3);

            cacheAsync.getAndPutIfAbsent("key3", 4);

            assertEquals((Integer)3, cacheAsync.<Integer>future().get());
        }

        cache.localEvict(Collections.singleton("key2"));

        if (!isLoadPreviousValue())
            cache.get("key2");

        // Same checks inside tx.
        tx = txShouldBeUsed() ? transactions().txStart() : null;

        try {
            cacheAsync.getAndPutIfAbsent("key2", 3);

            assertEquals(1, cacheAsync.future().get());

            if (tx != null)
                tx.commit();

            assertEquals((Integer)1, cache.get("key2"));
        }
        finally {
            if (tx != null)
                tx.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void _testPutIfAbsent() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        assertNull(cache.get("key"));
        assert cache.putIfAbsent("key", 1);
        assert cache.get("key") != null && cache.get("key") == 1;
        assert !cache.putIfAbsent("key", 2);
        assert cache.get("key") != null && cache.get("key") == 1;

        if (!storeEnabled())
            return;

        // Check swap.
        cache.put("key2", 1);

        cache.localEvict(Collections.singleton("key2"));

        if (!isLoadPreviousValue())
            cache.get("key2");

        assertFalse(cache.putIfAbsent("key2", 3));

        // Check db.
        if (storeEnabled() && isLoadPreviousValue() && !isMultiJvm()) {
            putToStore("key3", 3);

            assertFalse(cache.putIfAbsent("key3", 4));
        }

        cache.localEvict(Collections.singleton("key2"));

        if (!isLoadPreviousValue())
            cache.get("key2");

        // Same checks inside tx.
        Transaction tx = txShouldBeUsed() ? transactions().txStart() : null;

        try {
            assertFalse(cache.putIfAbsent("key2", 3));

            if (tx != null)
                tx.commit();

            assertEquals((Integer)1, cache.get("key2"));
        }
        finally {
            if (tx != null)
                tx.close();
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void _testPutxIfAbsentAsync() throws Exception {
        if (txShouldBeUsed())
            checkPutxIfAbsentAsync(true);
    }

    /**
     * @throws Exception In case of error.
     */
    public void _testPutxIfAbsentAsyncNoTx() throws Exception {
        checkPutxIfAbsentAsync(false);
    }

    /**
     * @param  inTx In tx flag.
     * @throws Exception If failed.
     */
    private void checkPutxIfAbsentAsync(boolean inTx) throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        IgniteCache<String, Integer> cacheAsync = cache.withAsync();

        cacheAsync.putIfAbsent("key", 1);

        IgniteFuture<Boolean> fut1 = cacheAsync.future();

        assert fut1.get();
        assert cache.get("key") != null && cache.get("key") == 1;

        cacheAsync.putIfAbsent("key", 2);

        IgniteFuture<Boolean> fut2 = cacheAsync.future();

        assert !fut2.get();
        assert cache.get("key") != null && cache.get("key") == 1;

        if (!storeEnabled())
            return;

        // Check swap.
        cache.put("key2", 1);

        cache.localEvict(Collections.singleton("key2"));

        if (!isLoadPreviousValue())
            cache.get("key2");

        cacheAsync.putIfAbsent("key2", 3);

        assertFalse(cacheAsync.<Boolean>future().get());

        // Check db.
        if (storeEnabled() && isLoadPreviousValue() && !isMultiJvm()) {
            putToStore("key3", 3);

            cacheAsync.putIfAbsent("key3", 4);

            assertFalse(cacheAsync.<Boolean>future().get());
        }

        cache.localEvict(Collections.singletonList("key2"));

        if (!isLoadPreviousValue())
            cache.get("key2");

        // Same checks inside tx.
        Transaction tx = inTx ? transactions().txStart() : null;

        try {
            cacheAsync.putIfAbsent("key2", 3);

            assertFalse(cacheAsync.<Boolean>future().get());

            if (storeEnabled() && isLoadPreviousValue() && !isMultiJvm()) {
                cacheAsync.putIfAbsent("key3", 4);

                assertFalse(cacheAsync.<Boolean>future().get());
            }

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        assertEquals((Integer)1, cache.get("key2"));

        if (storeEnabled() && isLoadPreviousValue() && !isMultiJvm())
            assertEquals((Integer)3, cache.get("key3"));
    }

    /**
     * @throws Exception In case of error.
     */
    public void _testPutIfAbsentAsyncConcurrent() throws Exception {
        IgniteCache cacheAsync = jcache().withAsync();

        cacheAsync.putIfAbsent("key1", 1);

        IgniteFuture<Boolean> fut1 = cacheAsync.future();

        cacheAsync.putIfAbsent("key2", 2);

        IgniteFuture<Boolean> fut2 = cacheAsync.future();

        assert fut1.get();
        assert fut2.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void _testGetAndReplace() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key", 1);

        assert cache.get("key") == 1;

        info("key 1 -> 2");

        assert cache.getAndReplace("key", 2) == 1;

        assert cache.get("key") == 2;

        assert cache.getAndReplace("wrong", 0) == null;

        assert cache.get("wrong") == null;

        info("key 0 -> 3");

        assert !cache.replace("key", 0, 3);

        assert cache.get("key") == 2;

        info("key 0 -> 3");

        assert !cache.replace("key", 0, 3);

        assert cache.get("key") == 2;

        info("key 2 -> 3");

        assert cache.replace("key", 2, 3);

        assert cache.get("key") == 3;

        // TODO delete the check and rewrite test.
        if (!storeEnabled())
            return;

        info("evict key");

        cache.localEvict(Collections.singleton("key"));

        info("key 3 -> 4");

        if (!isLoadPreviousValue())
            cache.get("key");

        assert cache.replace("key", 3, 4);

        assert cache.get("key") == 4;

        if (storeEnabled() && isLoadPreviousValue() && !isMultiJvm()) {
            putToStore("key2", 5);

            info("key2 5 -> 6");

            assert cache.replace("key2", 5, 6);
        }

        for (int i = 0; i < gridCount(); i++) {
            info("Peek key on grid [i=" + i + ", nodeId=" + grid(i).localNode().id() +
                ", peekVal=" + grid(i).cache(cacheName()).localPeek("key", ONHEAP) + ']');

            info("Peek key2 on grid [i=" + i + ", nodeId=" + grid(i).localNode().id() +
                ", peekVal=" + grid(i).cache(cacheName()).localPeek("key2", ONHEAP) + ']');
        }

        if (storeEnabled() && isLoadPreviousValue() && !isMultiJvm())
            assertEquals((Integer)6, cache.get("key2"));

        cache.localEvict(Collections.singleton("key"));

        if (!isLoadPreviousValue())
            cache.get("key");

        Transaction tx = txShouldBeUsed() ? transactions().txStart() : null;

        try {
            assert cache.replace("key", 4, 5);

            if (tx != null)
                tx.commit();

            assert cache.get("key") == 5;
        }
        finally {
            if (tx != null)
                tx.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void _testReplace() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key", 1);

        assert cache.get("key") == 1;

        assert cache.replace("key", 2);

        assert cache.get("key") == 2;

        assert !cache.replace("wrong", 2);

        // TODO delete the check and rewrite test.
        if (!storeEnabled())
            return;

        // TODO improve smartTestEvict
        cache.localEvict(Collections.singleton("key"));

        if (!isLoadPreviousValue())
            assert cache.get("key") == 2;

        assert cache.replace("key", 4);

        assert cache.get("key") == 4;

        if (storeEnabled() && isLoadPreviousValue() && !isMultiJvm()) {
            putToStore("key2", 5);

            cache.replace("key2", 6);

            assertEquals((Integer)6, cache.get("key2"));
        }

        cache.localEvict(Collections.singleton("key"));

        if (!isLoadPreviousValue())
            assert cache.get("key") == 4;

        Transaction tx = txShouldBeUsed() ? transactions().txStart() : null;

        try {
            assert cache.replace("key", 5);

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        assert cache.get("key") == 5;
    }

    /**
     * @throws Exception If failed.
     */
    public void _testGetAndReplaceAsync() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        IgniteCache<String, Integer> cacheAsync = cache.withAsync();

        cache.put("key", 1);

        assert cache.get("key") == 1;

        cacheAsync.getAndReplace("key", 2);

        assert cacheAsync.<Integer>future().get() == 1;

        assert cache.get("key") == 2;

        cacheAsync.getAndReplace("wrong", 0);

        assert cacheAsync.future().get() == null;

        assert cache.get("wrong") == null;

        cacheAsync.replace("key", 0, 3);

        assert !cacheAsync.<Boolean>future().get();

        assert cache.get("key") == 2;

        cacheAsync.replace("key", 0, 3);

        assert !cacheAsync.<Boolean>future().get();

        assert cache.get("key") == 2;

        cacheAsync.replace("key", 2, 3);

        assert cacheAsync.<Boolean>future().get();

        assert cache.get("key") == 3;

        // TODO delete the check and rewrite test.
        if (!storeEnabled())
            return;

        cache.localEvict(Collections.singleton("key"));

        if (!isLoadPreviousValue())
            cache.get("key");

        cacheAsync.replace("key", 3, 4);

        assert cacheAsync.<Boolean>future().get();

        assert cache.get("key") == 4;

        if (storeEnabled() && isLoadPreviousValue() && !isMultiJvm()) {
            putToStore("key2", 5);

            cacheAsync.replace("key2", 5, 6);

            assert cacheAsync.<Boolean>future().get();

            assertEquals((Integer)6, cache.get("key2"));
        }

        cache.localEvict(Collections.singleton("key"));

        if (!isLoadPreviousValue())
            cache.get("key");

        Transaction tx = txShouldBeUsed() ? transactions().txStart() : null;

        try {
            cacheAsync.replace("key", 4, 5);

            assert cacheAsync.<Boolean>future().get();

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        assert cache.get("key") == 5;
    }

    /**
     * @throws Exception If failed.
     */
    public void _testReplacexAsync() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        IgniteCache<String, Integer> cacheAsync = cache.withAsync();

        cache.put("key", 1);

        assert cache.get("key") == 1;

        cacheAsync.replace("key", 2);

        assert cacheAsync.<Boolean>future().get();

        info("Finished replace.");

        assertEquals((Integer)2, cache.get("key"));

        cacheAsync.replace("wrond", 2);

        assert !cacheAsync.<Boolean>future().get();

        // TODO delete the check and rewrite test.
        if (!storeEnabled())
            return;

        cache.localEvict(Collections.singleton("key"));

        if (!isLoadPreviousValue())
            cache.get("key");

        cacheAsync.replace("key", 4);

        assert cacheAsync.<Boolean>future().get();

        assert cache.get("key") == 4;

        if (storeEnabled() && isLoadPreviousValue() && !isMultiJvm()) {
            putToStore("key2", 5);

            cacheAsync.replace("key2", 6);

            assert cacheAsync.<Boolean>future().get();

            assert cache.get("key2") == 6;
        }

        cache.localEvict(Collections.singleton("key"));

        if (!isLoadPreviousValue())
            cache.get("key");

        Transaction tx = txShouldBeUsed() ? transactions().txStart() : null;

        try {
            cacheAsync.replace("key", 5);

            assert cacheAsync.<Boolean>future().get();

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        assert cache.get("key") == 5;
    }

    /**
     * @throws Exception In case of error.
     */
    public void _testGetAndRemove() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key1", 1);
        cache.put("key2", 2);

        assert !cache.remove("key1", 0);
        assert cache.get("key1") != null && cache.get("key1") == 1;
        assert cache.remove("key1", 1);
        assert cache.get("key1") == null;
        assert cache.getAndRemove("key2") == 2;
        assert cache.get("key2") == null;
        assert cache.getAndRemove("key2") == null;
    }

    /**
     * @throws Exception If failed.
     */
    public void _testGetAndRemoveObject() throws Exception {
        IgniteCache<String, SerializableObject> cache = ignite(0).cache(cacheName());

        SerializableObject val1 = new SerializableObject(1);
        SerializableObject val2 = new SerializableObject(2);

        cache.put("key1", val1);
        cache.put("key2", val2);

        assert !cache.remove("key1", new SerializableObject(0));

        SerializableObject oldVal = cache.get("key1");

        assert oldVal != null && F.eq(val1, oldVal);

        assert cache.remove("key1");

        assert cache.get("key1") == null;

        SerializableObject oldVal2 = cache.getAndRemove("key2");

        assert F.eq(val2, oldVal2);

        assert cache.get("key2") == null;
        assert cache.getAndRemove("key2") == null;
    }

    /**
     * @throws Exception If failed.
     */
    public void _testGetAndPutSerializableObject() throws Exception {
        IgniteCache<String, SerializableObject> cache = ignite(0).cache(cacheName());

        SerializableObject val1 = new SerializableObject(1);
        SerializableObject val2 = new SerializableObject(2);

        cache.put("key1", val1);

        SerializableObject oldVal = cache.get("key1");

        assertEquals(val1, oldVal);

        oldVal = cache.getAndPut("key1", val2);

        assertEquals(val1, oldVal);

        SerializableObject updVal = cache.get("key1");

        assertEquals(val2, updVal);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testDeletedEntriesFlag() throws Exception {
        if (cacheMode() != LOCAL && cacheMode() != REPLICATED && memoryMode() != OFFHEAP_TIERED) {
            final int cnt = 3;

            IgniteCache<String, Integer> cache = jcache();

            for (int i = 0; i < cnt; i++)
                cache.put(String.valueOf(i), i);

            for (int i = 0; i < cnt; i++)
                cache.remove(String.valueOf(i));

            for (int g = 0; g < gridCount(); g++)
                executeOnLocalOrRemoteJvm(g, new CheckEntriesDeletedTask(cnt, cacheName()));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void _testRemoveLoad() throws Exception {
        if (isMultiJvm())
            fail("https://issues.apache.org/jira/browse/IGNITE-1088");

        if (!storeEnabled())
            return;

        int cnt = 10;

        Set<String> keys = new HashSet<>();

        for (int i = 0; i < cnt; i++)
            keys.add(String.valueOf(i));

        jcache().removeAll(keys);

        for (String key : keys)
            putToStore(key, Integer.parseInt(key));

        for (int g = 0; g < gridCount(); g++)
            grid(g).cache(cacheName()).localLoadCache(null);

        for (int g = 0; g < gridCount(); g++) {
            for (int i = 0; i < cnt; i++) {
                String key = String.valueOf(i);

                if (grid(0).affinity(cacheName()).mapKeyToPrimaryAndBackups(key).contains(grid(g).localNode()))
                    assertEquals((Integer)i, peek(jcache(g), key));
                else
                    assertNull(peek(jcache(g), key));
            }
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void _testRemoveAsync() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        IgniteCache<String, Integer> cacheAsync = cache.withAsync();

        cache.put("key1", 1);
        cache.put("key2", 2);

        cacheAsync.remove("key1", 0);

        assert !cacheAsync.<Boolean>future().get();

        assert cache.get("key1") != null && cache.get("key1") == 1;

        cacheAsync.remove("key1", 1);

        assert cacheAsync.<Boolean>future().get();

        assert cache.get("key1") == null;

        cacheAsync.getAndRemove("key2");

        assert cacheAsync.<Integer>future().get() == 2;

        assert cache.get("key2") == null;

        cacheAsync.getAndRemove("key2");

        assert cacheAsync.future().get() == null;
    }

    /**
     * @throws Exception In case of error.
     */
    public void _testRemove() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key1", 1);

        assert cache.remove("key1");
        assert cache.get("key1") == null;
        assert !cache.remove("key1");
    }

    /**
     * @throws Exception In case of error.
     */
    public void _testRemovexAsync() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        IgniteCache<String, Integer> cacheAsync = cache.withAsync();

        cache.put("key1", 1);

        cacheAsync.remove("key1");

        assert cacheAsync.<Boolean>future().get();

        assert cache.get("key1") == null;

        cacheAsync.remove("key1");

        assert !cacheAsync.<Boolean>future().get();
    }

    /**
     * @throws Exception In case of error.
     */
    public void _testGlobalRemoveAll() throws Exception {
        globalRemoveAll(false);
    }

    /**
     * @throws Exception In case of error.
     */
    public void testGlobalRemoveAllAsync() throws Exception {
        globalRemoveAll(true);
    }

    /**
     * @param async If {@code true} uses asynchronous operation.
     * @throws Exception In case of error.
     */
    private void globalRemoveAll(boolean async) throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key1", 1);
        cache.put("key2", 2);
        cache.put("key3", 3);

        checkSize(F.asSet("key1", "key2", "key3"));

        atomicClockModeDelay(cache);

        IgniteCache<String, Integer> asyncCache = cache.withAsync();

        if (async) {
            asyncCache.removeAll(F.asSet("key1", "key2"));

            asyncCache.future().get();
        }
        else
            cache.removeAll(F.asSet("key1", "key2"));

        checkSize(F.asSet("key3"));

        checkContainsKey(false, "key1");
        checkContainsKey(false, "key2");
        checkContainsKey(true, "key3");

        // Put values again.
        cache.put("key1", 1);
        cache.put("key2", 2);
        cache.put("key3", 3);

        atomicClockModeDelay(cache);

        if (async) {
            IgniteCache asyncCache0 = jcache(gridCount() > 1 ? 1 : 0).withAsync();

            asyncCache0.removeAll();

            asyncCache0.future().get();
        }
        else
            jcache(gridCount() > 1 ? 1 : 0).removeAll();

        assertEquals(0, cache.localSize());
        long entryCnt = hugeRemoveAllEntryCount();

        for (int i = 0; i < entryCnt; i++)
            cache.put(String.valueOf(i), i);

        for (int i = 0; i < entryCnt; i++)
            assertEquals(Integer.valueOf(i), cache.get(String.valueOf(i)));

        atomicClockModeDelay(cache);

        if (async) {
            asyncCache.removeAll();

            asyncCache.future().get();
        }
        else
            cache.removeAll();

        for (int i = 0; i < entryCnt; i++)
            assertNull(cache.get(String.valueOf(i)));
    }

    /**
     * @return Count of entries to be removed in removeAll() test.
     */
    protected long hugeRemoveAllEntryCount(){
        return 1000L;
    }

    /**
     * @throws Exception In case of error.
     */
    public void _testRemoveAllWithNulls() throws Exception {
        final IgniteCache<String, Integer> cache = jcache();

        final Set<String> c = new LinkedHashSet<>();

        c.add("key1");
        c.add(null);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.removeAll(c);

                return null;
            }
        }, NullPointerException.class, null);

        assertEquals(0, jcache().localSize());

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.removeAll(null);

                return null;
            }
        }, NullPointerException.class, null);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.remove(null);

                return null;
            }
        }, NullPointerException.class, null);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.getAndRemove(null);

                return null;
            }
        }, NullPointerException.class, null);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.remove("key1", null);

                return null;
            }
        }, NullPointerException.class, null);
    }

    /**
     * @throws Exception In case of error.
     */
    public void _testRemoveAllDuplicates() throws Exception {
        jcache().removeAll(ImmutableSet.of("key1", "key1", "key1"));
    }

    /**
     * @throws Exception In case of error.
     */
    public void _testRemoveAllDuplicatesTx() throws Exception {
        if (txShouldBeUsed()) {
            try (Transaction tx = transactions().txStart()) {
                jcache().removeAll(ImmutableSet.of("key1", "key1", "key1"));

                tx.commit();
            }
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void _testRemoveAllEmpty() throws Exception {
        jcache().removeAll();
    }

    /**
     * @throws Exception In case of error.
     */
    public void _testRemoveAllAsync() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        IgniteCache<String, Integer> cacheAsync = cache.withAsync();

        cache.put("key1", 1);
        cache.put("key2", 2);
        cache.put("key3", 3);

        checkSize(F.asSet("key1", "key2", "key3"));

        cacheAsync.removeAll(F.asSet("key1", "key2"));

        assertNull(cacheAsync.future().get());

        checkSize(F.asSet("key3"));

        checkContainsKey(false, "key1");
        checkContainsKey(false, "key2");
        checkContainsKey(true, "key3");
    }

    /**
     * @throws Exception In case of error.
     */
    public void _testLoadAll() throws Exception {
        if (!storeEnabled())
            return;

        IgniteCache<String, Integer> cache = jcache();

        Set<String> keys = new HashSet<>(primaryKeysForCache(cache, 2));

        for (String key : keys)
            assertNull(cache.localPeek(key, ONHEAP));

        Map<String, Integer> vals = new HashMap<>();

        int i = 0;

        for (String key : keys) {
            cache.put(key, i);

            vals.put(key, i);

            i++;
        }

        for (String key : keys)
            assertEquals(vals.get(key), peek(cache, key));

        cache.clear();

        for (String key : keys)
            assertNull(peek(cache, key));

        loadAll(cache, keys, true);

        for (String key : keys)
            assertEquals(vals.get(key), peek(cache, key));
    }

    /**
     * @throws Exception If failed.
     */
    public void _testRemoveAfterClear() throws Exception {
        IgniteEx ignite = grid(0);

        boolean affNode = ignite.context().cache().internalCache(cacheName()).context().affinityNode();

        if (!affNode) {
            if (gridCount() < 2)
                return;

            ignite = grid(1);
        }

        IgniteCache<Integer, Integer> cache = ignite.cache(cacheName());

        int key = 0;

        Collection<Integer> keys = new ArrayList<>();

        for (int k = 0; k < 2; k++) {
            while (!ignite.affinity(cacheName()).isPrimary(ignite.localNode(), key))
                key++;

            keys.add(key);

            key++;
        }

        info("Keys: " + keys);

        for (Integer k : keys)
            cache.put(k, k);

        cache.clear();

        for (int g = 0; g < gridCount(); g++) {
            Ignite grid0 = grid(g);

            grid0.cache(cacheName()).removeAll();

            assertTrue(grid0.cache(cacheName()).localSize() == 0);
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void _testClear() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        Set<String> keys = new HashSet<>(primaryKeysForCache(cache, 3));

        for (String key : keys)
            assertNull(cache.get(key));

        Map<String, Integer> vals = new HashMap<>(keys.size());

        int i = 0;

        for (String key : keys) {
            cache.put(key, i);

            vals.put(key, i);

            i++;
        }

        for (String key : keys)
            assertEquals(vals.get(key), peek(cache, key));

        cache.clear();

        for (String key : keys)
            assertNull(peek(cache, key));

        for (i = 0; i < gridCount(); i++)
            jcache(i).clear();

        for (i = 0; i < gridCount(); i++)
            assert jcache(i).localSize() == 0;

        for (Map.Entry<String, Integer> entry : vals.entrySet())
            cache.put(entry.getKey(), entry.getValue());

        for (String key : keys)
            assertEquals(vals.get(key), peek(cache, key));

        String first = F.first(keys);

        if (lockingEnabled()) {
            Lock lock = cache.lock(first);

            lock.lock();

            try {
                cache.clear();

                GridCacheContext<String, Integer> cctx = context(0);

                GridCacheEntryEx entry = cctx.isNear() ? cctx.near().dht().peekEx(first) :
                    cctx.cache().peekEx(first);

                assertNotNull(entry);
            }
            finally {
                lock.unlock();
            }
        }
        else {
            cache.clear();

            cache.put(first, vals.get(first));
        }

        cache.clear();

        assert cache.localSize() == 0 : "Values after clear.";

        i = 0;

        for (String key : keys) {
            cache.put(key, i);

            vals.put(key, i);

            i++;
        }

        cache.put("key1", 1);
        cache.put("key2", 2);

        cache.localEvict(Sets.union(ImmutableSet.of("key1", "key2"), keys));

        assert cache.localSize(ONHEAP) == 0;

        cache.clear();

        cache.localPromote(ImmutableSet.of("key2", "key1"));

        assert cache.localPeek("key1", ONHEAP) == null;
        assert cache.localPeek("key2", ONHEAP) == null;
    }

    /**
     * @param keys0 Keys to check.
     * @throws IgniteCheckedException If failed.
     */
    protected void checkUnlocked(final Collection<String> keys0) throws IgniteCheckedException {
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                try {
                    for (int i = 0; i < gridCount(); i++) {
                        GridCacheAdapter<Object, Object> cache = ((IgniteKernal)ignite(i)).internalCache(cacheName());

                        for (String key : keys0) {
                            GridCacheEntryEx entry = cache.peekEx(key);

                            if (entry != null) {
                                if (entry.lockedByAny()) {
                                    info("Entry is still locked [i=" + i + ", entry=" + entry + ']');

                                    return false;
                                }
                            }

                            if (cache.isNear()) {
                                entry = cache.context().near().dht().peekEx(key);

                                if (entry != null) {
                                    if (entry.lockedByAny()) {
                                        info("Entry is still locked [i=" + i + ", entry=" + entry + ']');

                                        return false;
                                    }
                                }
                            }
                        }
                    }

                    return true;
                }
                catch (GridCacheEntryRemovedException ignore) {
                    info("Entry was removed, will retry");

                    return false;
                }
            }
        }, 10_000);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testGlobalClearAll() throws Exception {
        globalClearAll(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testGlobalClearAllAsync() throws Exception {
        globalClearAll(true);
    }

    /**
     * @param async If {@code true} uses async method.
     * @throws Exception If failed.
     */
    protected void globalClearAll(boolean async) throws Exception {
        // Save entries only on their primary nodes. If we didn't do so, clearLocally() will not remove all entries
        // because some of them were blocked due to having readers.
        for (int i = 0; i < gridCount(); i++) {
            for (String key : primaryKeysForCache(jcache(i), 3, 100_000))
                jcache(i).put(key, 1);
        }

        if (async) {
            IgniteCache asyncCache = jcache().withAsync();

            asyncCache.clear();

            asyncCache.future().get();
        }
        else
            jcache().clear();

        for (int i = 0; i < gridCount(); i++)
            assert jcache(i).localSize() == 0;
    }

    /**
     * @throws Exception In case of error.
     */
    @SuppressWarnings("BusyWait")
    public void _testLockUnlock() throws Exception {
        if (lockingEnabled()) {
            final CountDownLatch lockCnt = new CountDownLatch(1);
            final CountDownLatch unlockCnt = new CountDownLatch(1);

            grid(0).events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    switch (evt.type()) {
                        case EVT_CACHE_OBJECT_LOCKED:
                            lockCnt.countDown();

                            break;
                        case EVT_CACHE_OBJECT_UNLOCKED:
                            unlockCnt.countDown();

                            break;
                    }

                    return true;
                }
            }, EVT_CACHE_OBJECT_LOCKED, EVT_CACHE_OBJECT_UNLOCKED);

            IgniteCache<String, Integer> cache = jcache();

            String key = primaryKeysForCache(cache, 1).get(0);

            cache.put(key, 1);

            assert !cache.isLocalLocked(key, false);

            Lock lock = cache.lock(key);

            lock.lock();

            try {
                lockCnt.await();

                assert cache.isLocalLocked(key, false);
            }
            finally {
                lock.unlock();
            }

            unlockCnt.await();

            for (int i = 0; i < 100; i++)
                if (cache.isLocalLocked(key, false))
                    Thread.sleep(10);
                else
                    break;

            assert !cache.isLocalLocked(key, false);
        }
    }

    /**
     * @throws Exception In case of error.
     */
    @SuppressWarnings("BusyWait")
    public void _testLockUnlockAll() throws Exception {
        if (lockingEnabled()) {
            IgniteCache<String, Integer> cache = jcache();

            cache.put("key1", 1);
            cache.put("key2", 2);

            assert !cache.isLocalLocked("key1", false);
            assert !cache.isLocalLocked("key2", false);

            Lock lock1_2 = cache.lockAll(ImmutableSet.of("key1", "key2"));

            lock1_2.lock();

            try {
                assert cache.isLocalLocked("key1", false);
                assert cache.isLocalLocked("key2", false);
            }
            finally {
                lock1_2.unlock();
            }

            for (int i = 0; i < 100; i++)
                if (cache.isLocalLocked("key1", false) || cache.isLocalLocked("key2", false))
                    Thread.sleep(10);
                else
                    break;

            assert !cache.isLocalLocked("key1", false);
            assert !cache.isLocalLocked("key2", false);

            lock1_2.lock();

            try {
                assert cache.isLocalLocked("key1", false);
                assert cache.isLocalLocked("key2", false);
            }
            finally {
                lock1_2.unlock();
            }

            for (int i = 0; i < 100; i++)
                if (cache.isLocalLocked("key1", false) || cache.isLocalLocked("key2", false))
                    Thread.sleep(10);
                else
                    break;

            assert !cache.isLocalLocked("key1", false);
            assert !cache.isLocalLocked("key2", false);
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void _testPeek() throws Exception {
        Ignite ignite = primaryIgnite("key");
        IgniteCache<String, Integer> cache = ignite.cache(cacheName());

        assert peek(cache, "key") == null;

        cache.put("key", 1);

        cache.replace("key", 2);

        assertEquals(2, peek(cache, "key").intValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void _testPeekTxRemoveOptimistic() throws Exception {
        checkPeekTxRemove(OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testPeekTxRemovePessimistic() throws Exception {
        checkPeekTxRemove(PESSIMISTIC);
    }

    /**
     * @param concurrency Concurrency.
     * @throws Exception If failed.
     */
    private void checkPeekTxRemove(TransactionConcurrency concurrency) throws Exception {
        if (txShouldBeUsed()) {
            Ignite ignite = primaryIgnite("key");
            IgniteCache<String, Integer> cache = ignite.cache(cacheName());

            cache.put("key", 1);

            try (Transaction tx = ignite.transactions().txStart(concurrency, READ_COMMITTED)) {
                cache.remove("key");

                assertNull(cache.get("key")); // localPeek ignores transactions.
                assertNotNull(peek(cache, "key")); // localPeek ignores transactions.

                tx.commit();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void _testPeekRemove() throws Exception {
        IgniteCache<String, Integer> cache = primaryCache("key");

        cache.put("key", 1);
        cache.remove("key");

        assertNull(peek(cache, "key"));
    }

    /**
     * @throws Exception In case of error.
     */
    public void _testEvictExpired() throws Exception {
        final IgniteCache<String, Integer> cache = jcache();

        final String key = primaryKeysForCache(cache, 1).get(0);

        cache.put(key, 1);

        assertEquals((Integer)1, cache.get(key));

        long ttl = 500;

        final ExpiryPolicy expiry = new TouchedExpiryPolicy(new Duration(MILLISECONDS, ttl));

        grid(0).cache(cacheName()).withExpiryPolicy(expiry).put(key, 1);

        boolean wait = waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return cache.localPeek(key) == null;
            }
        }, ttl + 1000);

        assertTrue("Failed to wait for entry expiration.", wait);

        // Expired entry should not be swapped.
        cache.localEvict(Collections.singleton(key));

        assertNull(peek(cache, "key"));

        cache.localPromote(Collections.singleton(key));

        assertNull(cache.localPeek(key, ONHEAP));

        assertTrue(cache.localSize() == 0);

        if (storeEnabled()) {
            load(cache, key, true);

            Affinity<String> aff = ignite(0).affinity(cacheName());

            for (int i = 0; i < gridCount(); i++) {
                if (aff.isPrimary(grid(i).cluster().localNode(), key))
                    assertEquals((Integer)1, peek(jcache(i), key));

                if (aff.isBackup(grid(i).cluster().localNode(), key))
                    assertEquals((Integer)1, peek(jcache(i), key));
            }
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void _testPeekExpired() throws Exception {
        final IgniteCache<String, Integer> c = jcache();

        final String key = primaryKeysForCache(c, 1).get(0);

        info("Using key: " + key);

        c.put(key, 1);

        assertEquals(Integer.valueOf(1), peek(c, key));

        int ttl = 500;

        final ExpiryPolicy expiry = new TouchedExpiryPolicy(new Duration(MILLISECONDS, ttl));

        c.withExpiryPolicy(expiry).put(key, 1);

        Thread.sleep(ttl + 100);

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return peek(c, key) == null;
            }
        }, 2000);

        assert peek(c, key) == null;

        assert c.localSize() == 0 : "Cache is not empty.";
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void _testPeekExpiredTx() throws Exception {
        if (txShouldBeUsed()) {
            final IgniteCache<String, Integer> c = jcache();

            final String key = "1";
            int ttl = 500;

            try (Transaction tx = grid(0).transactions().txStart()) {
                final ExpiryPolicy expiry = new TouchedExpiryPolicy(new Duration(MILLISECONDS, ttl));

                grid(0).cache(cacheName()).withExpiryPolicy(expiry).put(key, 1);

                tx.commit();
            }

            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return peek(c, key) == null;
                }
            }, 2000);

            assertNull(peek(c, key));

            assert c.localSize() == 0;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void _testTtlTx() throws Exception {
        if (txShouldBeUsed())
            checkTtl(true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testTtlNoTx() throws Exception {
        checkTtl(false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testTtlNoTxOldEntry() throws Exception {
        checkTtl(false, true);
    }

    /**
     * @param inTx In tx flag.
     * @param oldEntry {@code True} to check TTL on old entry, {@code false} on new.
     * @throws Exception If failed.
     */
    private void checkTtl(boolean inTx, boolean oldEntry) throws Exception {
        if (isMultiJvm())
            fail("https://issues.apache.org/jira/browse/IGNITE-1089");

        if (memoryMode() == OFFHEAP_TIERED)
            return;

        int ttl = 1000;

        final ExpiryPolicy expiry = new TouchedExpiryPolicy(new Duration(MILLISECONDS, ttl));

        final IgniteCache<String, Integer> c = jcache();

        final String key = primaryKeysForCache(jcache(), 1).get(0);

        GridCacheAdapter<String, Integer> internalCache = internalCache(fullCache());

        if (internalCache.isNear())
            internalCache = internalCache.context().near().dht();

        GridCacheEntryEx entry;

        if (oldEntry) {
            c.put(key, 1);

            entry = internalCache.peekEx(key);

            assert entry != null;

            assertEquals(0, entry.ttl());
            assertEquals(0, entry.expireTime());
        }

        long startTime = System.currentTimeMillis();

        if (inTx) {
            // Rollback transaction for the first time.
            Transaction tx = transactions().txStart();

            try {
                jcache().withExpiryPolicy(expiry).put(key, 1);
            }
            finally {
                tx.rollback();
            }

            if (oldEntry) {
                entry = internalCache.peekEx(key);

                assertEquals(0, entry.ttl());
                assertEquals(0, entry.expireTime());
            }
        }

        // Now commit transaction and check that ttl and expire time have been saved.
        Transaction tx = inTx ? transactions().txStart() : null;

        try {
            jcache().withExpiryPolicy(expiry).put(key, 1);

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        long[] expireTimes = new long[gridCount()];

        for (int i = 0; i < gridCount(); i++) {
            if (grid(i).affinity(cacheName()).isPrimaryOrBackup(grid(i).localNode(), key)) {
                GridCacheAdapter<Object, Object> cache = internalCache(jcache(i));

                if (cache.context().isNear())
                    cache = cache.context().near().dht();

                GridCacheEntryEx curEntry = cache.peekEx(key);

                assertEquals(ttl, curEntry.ttl());

                assert curEntry.expireTime() > startTime;

                expireTimes[i] = curEntry.expireTime();
            }
        }

        // One more update from the same cache entry to ensure that expire time is shifted forward.
        U.sleep(100);

        tx = inTx ? transactions().txStart() : null;

        try {
            jcache().withExpiryPolicy(expiry).put(key, 2);

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        for (int i = 0; i < gridCount(); i++) {
            if (grid(i).affinity(cacheName()).isPrimaryOrBackup(grid(i).localNode(), key)) {
                GridCacheAdapter<Object, Object> cache = internalCache(jcache(i));

                if (cache.context().isNear())
                    cache = cache.context().near().dht();

                GridCacheEntryEx curEntry = cache.peekEx(key);

                assertEquals(ttl, curEntry.ttl());

                assert curEntry.expireTime() > startTime;

                expireTimes[i] = curEntry.expireTime();
            }
        }

        // And one more direct update to ensure that expire time is shifted forward.
        U.sleep(100);

        tx = inTx ? transactions().txStart() : null;

        try {
            jcache().withExpiryPolicy(expiry).put(key, 3);

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        for (int i = 0; i < gridCount(); i++) {
            if (grid(i).affinity(cacheName()).isPrimaryOrBackup(grid(i).localNode(), key)) {
                GridCacheAdapter<Object, Object> cache = internalCache(jcache(i));

                if (cache.context().isNear())
                    cache = cache.context().near().dht();

                GridCacheEntryEx curEntry = cache.peekEx(key);

                assertEquals(ttl, curEntry.ttl());

                assert curEntry.expireTime() > startTime;

                expireTimes[i] = curEntry.expireTime();
            }
        }

        // And one more update to ensure that ttl is not changed and expire time is not shifted forward.
        U.sleep(100);

        log.info("Put 4");

        tx = inTx ? transactions().txStart() : null;

        try {
            jcache().put(key, 4);

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        log.info("Put 4 done");

        for (int i = 0; i < gridCount(); i++) {
            if (grid(i).affinity(cacheName()).isPrimaryOrBackup(grid(i).localNode(), key)) {
                GridCacheAdapter cache = internalCache(jcache(i));

                if (cache.context().isNear())
                    cache = cache.context().near().dht();

                GridCacheEntryEx curEntry = cache.peekEx(key);

                assertEquals(ttl, curEntry.ttl());
                assertEquals(expireTimes[i], curEntry.expireTime());
            }
        }

        // Avoid reloading from store.
        map.remove(key);

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicateX() {
            @SuppressWarnings("unchecked")
            @Override public boolean applyx() {
                try {
                    Integer val = c.get(key);

                    if (val != null) {
                        info("Value is in cache [key=" + key + ", val=" + val + ']');

                        return false;
                    }

                    // Get "cache" field from GridCacheProxyImpl.
                    GridCacheAdapter c0 = cacheFromCtx(c);

                    if (!c0.context().deferredDelete()) {
                        GridCacheEntryEx e0 = c0.peekEx(key);

                        return e0 == null || (e0.rawGet() == null && e0.valueBytes() == null);
                    }
                    else
                        return true;
                }
                catch (GridCacheEntryRemovedException e) {
                    throw new RuntimeException(e);
                }
            }
        }, Math.min(ttl * 10, getTestTimeout())));

        if (internalCache.isLocal())
            return;

        assert c.get(key) == null;

        internalCache = internalCache(fullCache());

        if (internalCache.isNear())
            internalCache = internalCache.context().near().dht();

        // Ensure that old TTL and expire time are not longer "visible".
        entry = internalCache.peekEx(key);

        assertEquals(0, entry.ttl());
        assertEquals(0, entry.expireTime());

        // Ensure that next update will not pick old expire time.

        tx = inTx ? transactions().txStart() : null;

        try {
            jcache().put(key, 10);

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        U.sleep(2000);

        entry = internalCache.peekEx(key);

        assertEquals((Integer)10, c.get(key));

        assertEquals(0, entry.ttl());
        assertEquals(0, entry.expireTime());
    }

    /**
     * @throws Exception In case of error.
     */
    public void _testLocalEvict() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        List<String> keys = primaryKeysForCache(cache, 3);

        String key1 = keys.get(0);
        String key2 = keys.get(1);
        String key3 = keys.get(2);

        cache.put(key1, 1);
        cache.put(key2, 2);
        cache.put(key3, 3);

        assert peek(cache, key1) == 1;
        assert peek(cache, key2) == 2;
        assert peek(cache, key3) == 3;

        cache.localEvict(F.asList(key1, key2));

        assert cache.localPeek(key1, ONHEAP) == null;
        assert cache.localPeek(key2, ONHEAP) == null;
        assert peek(cache, key3) == 3;

        if (storeEnabled()) {
            loadAll(cache, ImmutableSet.of(key1, key2), true);

            Affinity<String> aff = ignite(0).affinity(cacheName());

            for (int i = 0; i < gridCount(); i++) {
                if (aff.isPrimaryOrBackup(grid(i).cluster().localNode(), key1))
                    assertEquals((Integer)1, peek(jcache(i), key1));

                if (aff.isPrimaryOrBackup(grid(i).cluster().localNode(), key2))
                    assertEquals((Integer)2, peek(jcache(i), key2));

                if (aff.isPrimaryOrBackup(grid(i).cluster().localNode(), key3))
                    assertEquals((Integer)3, peek(jcache(i), key3));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void _testUnswap() throws Exception {
        // TODO implement testUnswap for case when offheap is enabled.
        if (swapEnabled() && !offheapEnabled()) {
            IgniteCache<String, Integer> cache = jcache();

            List<String> keys = primaryKeysForCache(jcache(), 3);

            String k1 = keys.get(0);
            String k2 = keys.get(1);
            String k3 = keys.get(2);

            cache.getAndPut(k1, 1);
            cache.getAndPut(k2, 2);
            cache.getAndPut(k3, 3);

            final AtomicInteger swapEvts = new AtomicInteger(0);
            final AtomicInteger unswapEvts = new AtomicInteger(0);

            Collection<String> locKeys = new HashSet<>();

            if (grid(0).context().cache().cache(cacheName()).context().affinityNode()) {
                Iterable<Cache.Entry<String, Integer>> entries = cache.localEntries(PRIMARY, BACKUP);

                for (Cache.Entry<String, Integer> entry : entries)
                    locKeys.add(entry.getKey());

                info("Local keys (primary + backup): " + locKeys);
            }

            for (int i = 0; i < gridCount(); i++)
                grid(i).events().localListen(
                    new SwapEvtsLocalListener(swapEvts, unswapEvts), EVT_CACHE_OBJECT_SWAPPED, EVT_CACHE_OBJECT_UNSWAPPED);

            cache.localEvict(F.asList(k2, k3));

            if (memoryMode() == OFFHEAP_TIERED) {
                assertNotNull(cache.localPeek(k1, ONHEAP, OFFHEAP));
                assertNotNull(cache.localPeek(k2, ONHEAP, OFFHEAP));
                assertNotNull(cache.localPeek(k3, ONHEAP, OFFHEAP));
            }
            else {
                assertNotNull(cache.localPeek(k1, ONHEAP, OFFHEAP));
                assertNull(cache.localPeek(k2, ONHEAP, OFFHEAP));
                assertNull(cache.localPeek(k3, ONHEAP, OFFHEAP));
            }

            int cnt = 0;

            if (locKeys.contains(k2) && swapEnabled()) {
                assertNull(cache.localPeek(k2, ONHEAP));

                cache.localPromote(Collections.singleton(k2));

                assertEquals((Integer)2, cache.localPeek(k2, ONHEAP));

                if (swapAfterLocalEvict())
                    cnt++;
            }
            else {
                cache.localPromote(Collections.singleton(k2));

                assertNull(cache.localPeek(k2, ONHEAP));
            }

            if (locKeys.contains(k3) && swapEnabled()) {
                assertNull(cache.localPeek(k3, ONHEAP));

                cache.localPromote(Collections.singleton(k3));

                assertEquals((Integer)3, cache.localPeek(k3, ONHEAP));

                if (swapAfterLocalEvict())
                    cnt++;
            }
            else {
                cache.localPromote(Collections.singleton(k3));

                assertNull(cache.localPeek(k3, ONHEAP));
            }

            assertEquals(cnt, swapEvts.get());
            assertEquals(cnt, unswapEvts.get());

            cache.localEvict(Collections.singleton(k1));

            assertEquals((Integer)1, cache.get(k1));

            if (locKeys.contains(k1) && swapAfterLocalEvict())
                cnt++;

            assertEquals(cnt, swapEvts.get());
            assertEquals(cnt, unswapEvts.get());

            cache.clear();

            // Check with multiple arguments.
            cache.getAndPut(k1, 1);
            cache.getAndPut(k2, 2);
            cache.getAndPut(k3, 3);

            swapEvts.set(0);
            unswapEvts.set(0);

            cache.localEvict(Collections.singleton(k2));
            cache.localEvict(Collections.singleton(k3));

            if (memoryMode() == OFFHEAP_TIERED) {
                assertNotNull(cache.localPeek(k1, ONHEAP, OFFHEAP));
                assertNotNull(cache.localPeek(k2, ONHEAP, OFFHEAP));
                assertNotNull(cache.localPeek(k3, ONHEAP, OFFHEAP));
            }
            else {
                assertNotNull(cache.localPeek(k1, ONHEAP, OFFHEAP));
                assertNull(cache.localPeek(k2, ONHEAP, OFFHEAP));
                assertNull(cache.localPeek(k3, ONHEAP, OFFHEAP));
            }

            cache.localPromote(F.asSet(k2, k3));

            cnt = 0;

            if (locKeys.contains(k2) && swapAfterLocalEvict())
                cnt++;

            if (locKeys.contains(k3) && swapAfterLocalEvict())
                cnt++;

            assertEquals(cnt, swapEvts.get());
            assertEquals(cnt, unswapEvts.get());
        }
    }

    /**
     * @param cache Cache.
     * @param k Key,
     */
    private void checkKeyAfterPut(IgniteCache<String, Integer> cache, String k) {
        if (memoryMode() == OFFHEAP_TIERED) {
            assertNotNull(cache.localPeek(k, OFFHEAP));
            assertNull(cache.localPeek(k, ONHEAP));
        }
        else {
            assertNotNull(cache.localPeek(k, ONHEAP));
            assertNull(cache.localPeek(k, OFFHEAP));
        }
    }

    /**
     * @param cache Cache.
     * @param k Key.
     */
    private void checkKeyAfterLocalEvict(IgniteCache<String, Integer> cache, String k) {
        switch (memoryMode()) {
            case ONHEAP_TIERED:
                assertNull(cache.localPeek(k, ONHEAP));
                assertEquals(offheapEnabled(), cache.localPeek(k, OFFHEAP) != null);

                break;
            case OFFHEAP_TIERED:
                assertNull(cache.localPeek(k, ONHEAP));
                assertNotNull(cache.localPeek(k, OFFHEAP));

                break;
            case OFFHEAP_VALUES:
                assertNull(cache.localPeek(k, ONHEAP, OFFHEAP));

                break;
            default:
                fail("Unexpected memory mode: " + memoryMode());
        }
    }

    /**
     * JUnit.
     */
    public void _testCacheProxy() {
        IgniteCache<String, Integer> cache = jcache();

        assert cache instanceof IgniteCacheProxy;
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void _testCompactExpired() throws Exception {
        final IgniteCache<String, Integer> cache = jcache();

        final String key = F.first(primaryKeysForCache(cache, 1));

        cache.put(key, 1);

        long ttl = 500;

        final ExpiryPolicy expiry = new TouchedExpiryPolicy(new Duration(MILLISECONDS, ttl));

        grid(0).cache(cacheName()).withExpiryPolicy(expiry).put(key, 1);

        waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return cache.localPeek(key) == null;
            }
        }, ttl + 1000);

        // Peek will actually remove entry from cache.
        assertNull(cache.localPeek(key));

        assert cache.localSize() == 0;

        // Clear readers, if any.
        cache.remove(key);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void _testOptimisticTxMissingKey() throws Exception {
        if (txShouldBeUsed()) {
            try (Transaction tx = transactions().txStart(OPTIMISTIC, READ_COMMITTED)) {
                // Remove missing key.
                assertFalse(jcache().remove(UUID.randomUUID().toString()));

                tx.commit();
            }
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void _testOptimisticTxMissingKeyNoCommit() throws Exception {
        if (txShouldBeUsed()) {
            try (Transaction tx = transactions().txStart(OPTIMISTIC, READ_COMMITTED)) {
                // Remove missing key.
                assertFalse(jcache().remove(UUID.randomUUID().toString()));

                tx.setRollbackOnly();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void _testOptimisticTxReadCommittedInTx() throws Exception {
        checkRemovexInTx(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testOptimisticTxRepeatableReadInTx() throws Exception {
        checkRemovexInTx(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testPessimisticTxReadCommittedInTx() throws Exception {
        checkRemovexInTx(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testPessimisticTxRepeatableReadInTx() throws Exception {
        checkRemovexInTx(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws Exception If failed.
     */
    private void checkRemovexInTx(final TransactionConcurrency concurrency, final TransactionIsolation isolation) throws Exception {
        if (txShouldBeUsed()) {
            final int cnt = 10;

            CU.inTx(ignite(0), jcache(), concurrency, isolation, new CIX1<IgniteCache<Object, Object>>() {
                @Override public void applyx(IgniteCache cache) {
                    for (int i = 0; i < cnt; i++)
                        cache.put("key" + i, i);
                }
            });

            CU.inTx(ignite(0), jcache(), concurrency, isolation, new CIX1<IgniteCache<Object, Object>>() {
                @Override public void applyx(IgniteCache<Object, Object> cache) {
                    for (int i = 0; i < cnt; i++)
                        assertEquals(new Integer(i), cache.get("key" + i));
                }
            });

            CU.inTx(ignite(0), jcache(), concurrency, isolation, new CIX1<IgniteCache<Object, Object>>() {
                @Override public void applyx(IgniteCache<Object, Object> cache) {
                    for (int i = 0; i < cnt; i++) {
                        boolean removed = cache.remove("key" + i);

                        // TODO: delete the following check when IGNITE-2590 will be fixed.
                        boolean bug2590 = cacheMode() == LOCAL && memoryMode() == OFFHEAP_TIERED
                            && concurrency == OPTIMISTIC && isolation == REPEATABLE_READ;

                        if (!bug2590)
                            assertTrue(removed);
                    }
                }
            });

            CU.inTx(ignite(0), jcache(), concurrency, isolation, new CIX1<IgniteCache<Object, Object>>() {
                @Override public void applyx(IgniteCache<Object, Object> cache) {
                    for (int i = 0; i < cnt; i++)
                        assertNull(cache.get("key" + i));
                }
            });
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void _testPessimisticTxMissingKey() throws Exception {
        if (txShouldBeUsed()) {
            try (Transaction tx = transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                // Remove missing key.
                assertFalse(jcache().remove(UUID.randomUUID().toString()));

                tx.commit();
            }
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void _testPessimisticTxMissingKeyNoCommit() throws Exception {
        if (txShouldBeUsed()) {
            try (Transaction tx = transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                // Remove missing key.
                assertFalse(jcache().remove(UUID.randomUUID().toString()));

                tx.setRollbackOnly();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void _testPessimisticTxRepeatableRead() throws Exception {
        if (txShouldBeUsed()) {
            try (Transaction ignored = transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                jcache().put("key", 1);

                assert jcache().get("key") == 1;
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void _testPessimisticTxRepeatableReadOnUpdate() throws Exception {
        if (txShouldBeUsed()) {
            try (Transaction ignored = transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                jcache().put("key", 1);

                assert jcache().getAndPut("key", 2) == 1;
            }
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void _testToMap() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        if (offheapTiered(cache))
            return;

        cache.put("key1", 1);
        cache.put("key2", 2);

        Map map = new HashMap<>();

        for (int i = 0; i < gridCount(); i++) {
            for (Cache.Entry entry : jcache(i))
                map.put(entry.getKey(), entry.getValue());
        }

        assert map.size() == 2;
        assert map.get("key1") == 1;
        assert map.get("key2") == 2;
    }

    /**
     * @param keys Expected keys.
     * @throws Exception If failed.
     */
    protected void checkSize(final Collection<String> keys) throws Exception {
        if (memoryMode() == OFFHEAP_TIERED)
            return;

        if (nearEnabled())
            assertEquals(keys.size(), jcache().localSize(CachePeekMode.ALL));
        else {
            for (int i = 0; i < gridCount(); i++)
                executeOnLocalOrRemoteJvm(i, new CheckEntriesTask(keys, cacheName()));
        }
    }

    /**
     * @param keys Expected keys.
     * @throws Exception If failed.
     */
    protected void checkKeySize(final Collection<String> keys) throws Exception {
        if (nearEnabled())
            assertEquals("Invalid key size: " + jcache().localSize(ALL),
                keys.size(), jcache().localSize(ALL));
        else {
            for (int i = 0; i < gridCount(); i++)
                executeOnLocalOrRemoteJvm(i, new CheckKeySizeTask(keys, cacheName()));
        }
    }

    /**
     * @param exp Expected value.
     * @param key Key.
     * @throws Exception If failed.
     */
    private void checkContainsKey(boolean exp, Object key) throws Exception {
        if (nearEnabled())
            assertEquals(exp, jcache().containsKey(key));
        else {
            boolean contains = false;

            for (int i = 0; i < gridCount(); i++)
                if (containsKey(jcache(i), key)) {
                    contains = true;

                    break;
                }

            assertEquals("Key: " + key, exp, contains);
        }
    }

    /**
     * @param key Key.
     */
    protected Ignite primaryIgnite(String key) {
        ClusterNode node = grid(0).affinity(cacheName()).mapKeyToNode(key);

        if (node == null)
            throw new IgniteException("Failed to find primary node.");

        UUID nodeId = node.id();

        for (int i = 0; i < gridCount(); i++) {
            if (grid(i).localNode().id().equals(nodeId))
                return ignite(i);
        }

        throw new IgniteException("Failed to find primary node.");
    }

    /**
     * @param key Key.
     * @return Cache.
     */
    protected IgniteCache<String, Integer> primaryCache(String key) {
        return primaryIgnite(key).cache(cacheName());
    }

    /**
     * @param cache Cache.
     * @param cnt Keys count.
     * @return Collection of keys for which given cache is primary.
     */
    protected List<String> primaryKeysForCache(IgniteCache cache, int cnt, int startFrom) {
        return executeOnLocalOrRemoteJvm(cache, new CheckPrimaryKeysTask(startFrom, cnt));
    }

    /**
     * @param cache Cache.
     * @param cnt Keys count.
     * @return Collection of keys for which given cache is primary.
     */
    protected List<TestObject> primaryTestObjectKeysForCache(IgniteCache cache, int cnt, DataMode mode) {
        return primaryTestObjectKeysForCache(cache, cnt, 1, mode);
    }

    /**
     * @param cache Cache.
     * @param cnt Keys count.
     * @return Collection of keys for which given cache is primary.
     */
    protected List<TestObject> primaryTestObjectKeysForCache(IgniteCache cache, int cnt, int startFrom, DataMode mode) {
        return executeOnLocalOrRemoteJvm(cache, new CheckPrimaryTestObjectKeysTask(startFrom, cnt, mode));
    }

    /**
     * @param cache Cache.
     * @param cnt Keys count.
     * @return Collection of keys for which given cache is primary.
     * @throws IgniteCheckedException If failed.
     */
    protected List<String> primaryKeysForCache(IgniteCache cache, int cnt)
        throws IgniteCheckedException {
        return primaryKeysForCache(cache, cnt, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testIterator() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(cacheName());

        final int KEYS = 1000;

        for (int i = 0; i < KEYS; i++)
            cache.put(i, i);

        // Try to initialize readers in case when near cache is enabled.
        for (int i = 0; i < gridCount(); i++) {
            cache = grid(i).cache(cacheName());

            for (int k = 0; k < KEYS; k++)
                assertEquals((Object)k, cache.get(k));
        }

        int cnt = 0;

        for (Cache.Entry e : cache)
            cnt++;

        assertEquals(KEYS, cnt);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testIgniteCacheIterator() throws Exception {
        IgniteCache<String, Integer> cache = jcache(0);

        Iterator<Cache.Entry<String, Integer>> it = cache.iterator();

        boolean hasNext = it.hasNext();

        if (hasNext)
            assertFalse("Cache has value: " + it.next(), hasNext);

        final int SIZE = 10_000;

        Map<String, Integer> entries = new HashMap<>();

        Map<String, Integer> putMap = new HashMap<>();

        for (int i = 0; i < SIZE; ++i) {
            String key = Integer.toString(i);

            putMap.put(key, i);

            entries.put(key, i);

            if (putMap.size() == 500) {
                cache.putAll(putMap);

                info("Puts finished: " + (i + 1));

                putMap.clear();
            }
        }

        cache.putAll(putMap);

        checkIteratorHasNext();

        checkIteratorCache(entries);

        checkIteratorRemove(cache, entries);

        checkIteratorEmpty(cache);
    }

    /**
     * If hasNext() is called repeatedly, it should return the same result.
     */
    private void checkIteratorHasNext() {
        Iterator<Cache.Entry<Object, Object>> iter = jcache(0).iterator();

        assertEquals(iter.hasNext(), iter.hasNext());

        while (iter.hasNext())
            iter.next();

        assertFalse(iter.hasNext());
    }

    /**
     * @param cache Cache.
     * @param entries Expected entries in the cache.
     */
    private void checkIteratorRemove(IgniteCache<String, Integer> cache, Map<String, Integer> entries) {
        // Check that we can remove element.
        String rmvKey = Integer.toString(5);

        removeCacheIterator(cache, rmvKey);

        entries.remove(rmvKey);

        assertFalse(cache.containsKey(rmvKey));
        assertNull(cache.get(rmvKey));

        checkIteratorCache(entries);

        // Check that we cannot call Iterator.remove() without next().
        final Iterator<Cache.Entry<Object, Object>> iter = jcache(0).iterator();

        assertTrue(iter.hasNext());

        iter.next();

        iter.remove();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Void call() throws Exception {
                iter.remove();

                return null;
            }
        }, IllegalStateException.class, null);
    }

    /**
     * @param cache Cache.
     * @param key Key to remove.
     */
    private void removeCacheIterator(IgniteCache<String, Integer> cache, String key) {
        Iterator<Cache.Entry<String, Integer>> iter = cache.iterator();

        int delCnt = 0;

        while (iter.hasNext()) {
            Cache.Entry<String, Integer> cur = iter.next();

            if (cur.getKey().equals(key)) {
                iter.remove();

                delCnt++;
            }
        }

        assertEquals(1, delCnt);
    }

    /**
     * @param entries Expected entries in the cache.
     */
    private void checkIteratorCache(Map<String, Integer> entries) {
        for (int i = 0; i < gridCount(); ++i)
            checkIteratorCache(jcache(i), entries);
    }

    /**
     * @param cache Cache.
     * @param entries Expected entries in the cache.
     */
    private void checkIteratorCache(IgniteCache cache, Map<String, Integer> entries) {
        Iterator<Cache.Entry<String, Integer>> iter = cache.iterator();

        int cnt = 0;

        while (iter.hasNext()) {
            Cache.Entry<String, Integer> cur = iter.next();

            assertTrue(entries.containsKey(cur.getKey()));
            assertEquals(entries.get(cur.getKey()), cur.getValue());

            cnt++;
        }

        assertEquals(entries.size(), cnt);
    }

    /**
     * Checks iterators are cleared.
     */
    private void checkIteratorsCleared() {
        for (int j = 0; j < gridCount(); j++)
            executeOnLocalOrRemoteJvm(j, new CheckIteratorTask(cacheName()));
    }

    /**
     * Checks iterators are cleared after using.
     *
     * @param cache Cache.
     * @throws Exception If failed.
     */
    private void checkIteratorEmpty(IgniteCache<String, Integer> cache) throws Exception {
        int cnt = 5;

        for (int i = 0; i < cnt; ++i) {
            Iterator<Cache.Entry<String, Integer>> iter = cache.iterator();

            iter.next();

            assert iter.hasNext();
        }

        System.gc();

        for (int i = 0; i < 10; i++) {
            try {
                cache.size(); // Trigger weak queue poll.

                checkIteratorsCleared();
            }
            catch (AssertionFailedError e) {
                if (i == 9)
                    throw e;

                log.info("Set iterators not cleared, will wait");

                Thread.sleep(1000);
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void _testLocalClearKey() throws Exception {
        addKeys();

        String keyToRmv = "key" + 25;

        Ignite g = primaryIgnite(keyToRmv);

        g.<String, Integer>cache(cacheName()).localClear(keyToRmv);

        checkLocalRemovedKey(keyToRmv);

        g.<String, Integer>cache(cacheName()).put(keyToRmv, 1);

        String keyToEvict = "key" + 30;

        g = primaryIgnite(keyToEvict);

        g.<String, Integer>cache(cacheName()).localEvict(Collections.singleton(keyToEvict));

        g.<String, Integer>cache(cacheName()).localClear(keyToEvict);

        checkLocalRemovedKey(keyToEvict);
    }

    /**
     * @param keyToRmv Removed key.
     */
    protected void checkLocalRemovedKey(String keyToRmv) {
        for (int i = 0; i < 500; ++i) {
            String key = "key" + i;

            boolean found = primaryIgnite(key).cache(cacheName()).localPeek(key) != null;

            if (keyToRmv.equals(key)) {
                Collection<ClusterNode> nodes = grid(0).affinity(cacheName()).mapKeyToPrimaryAndBackups(key);

                for (int j = 0; j < gridCount(); ++j) {
                    if (nodes.contains(grid(j).localNode()) && grid(j) != primaryIgnite(key))
                        assertTrue("Not found on backup removed key ", grid(j).cache(cacheName()).localPeek(key) != null);
                }

                assertFalse("Found removed key " + key, found);
            }
            else
                assertTrue("Not found key " + key, found);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void _testLocalClearKeys() throws Exception {
        Map<String, List<String>> keys = addKeys();

        Ignite g = grid(0);

        Set<String> keysToRmv = new HashSet<>();

        for (int i = 0; i < gridCount(); ++i) {
            List<String> gridKeys = keys.get(grid(i).name());

            if (gridKeys.size() > 2) {
                keysToRmv.add(gridKeys.get(0));

                keysToRmv.add(gridKeys.get(1));

                g = grid(i);

                break;
            }
        }

        assert keysToRmv.size() > 1;

        info("Will clear keys on node: " + g.cluster().localNode().id());

        g.<String, Integer>cache(cacheName()).localClearAll(keysToRmv);

        for (int i = 0; i < 500; ++i) {
            String key = "key" + i;

            Ignite ignite = primaryIgnite(key);

            boolean found = ignite.cache(cacheName()).localPeek(key) != null;

            if (keysToRmv.contains(key))
                assertFalse("Found removed key [key=" + key + ", node=" + ignite.cluster().localNode().id() + ']',
                    found);
            else
                assertTrue("Not found key " + key, found);
        }
    }

    /**
     * Add 500 keys to cache only on primaries nodes.
     *
     * @return Map grid's name to its primary keys.
     */
    protected Map<String, List<String>> addKeys() {
        // Save entries only on their primary nodes. If we didn't do so, clearLocally() will not remove all entries
        // because some of them were blocked due to having readers.
        Map<String, List<String>> keys = new HashMap<>();

        for (int i = 0; i < gridCount(); ++i)
            keys.put(grid(i).name(), new ArrayList<String>());

        for (int i = 0; i < 500; ++i) {
            String key = "key" + i;

            Ignite g = primaryIgnite(key);

            g.cache(cacheName()).put(key, "value" + i);

            keys.get(g.name()).add(key);
        }

        return keys;
    }

    /**
     * @throws Exception If failed.
     */
    public void _testGlobalClearKey() throws Exception {
        testGlobalClearKey(false, Arrays.asList("key25"));
    }

    /**
     * @throws Exception If failed.
     */
    public void _testGlobalClearKeyAsync() throws Exception {
        testGlobalClearKey(true, Arrays.asList("key25"));
    }

    /**
     * @throws Exception If failed.
     */
    public void _testGlobalClearKeys() throws Exception {
        testGlobalClearKey(false, Arrays.asList("key25", "key100", "key150"));
    }

    /**
     * @throws Exception If failed.
     */
    public void _testGlobalClearKeysAsync() throws Exception {
        testGlobalClearKey(true, Arrays.asList("key25", "key100", "key150"));
    }

    /**
     * @param async If {@code true} uses async method.
     * @param keysToRmv Keys to remove.
     * @throws Exception If failed.
     */
    protected void testGlobalClearKey(boolean async, Collection<String> keysToRmv) throws Exception {
        // Save entries only on their primary nodes. If we didn't do so, clearLocally() will not remove all entries
        // because some of them were blocked due to having readers.
        for (int i = 0; i < 500; ++i) {
            String key = "key" + i;

            Ignite g = primaryIgnite(key);

            g.cache(cacheName()).put(key, "value" + i);
        }

        if (async) {
            IgniteCache asyncCache = jcache().withAsync();

            if (keysToRmv.size() == 1)
                asyncCache.clear(F.first(keysToRmv));
            else
                asyncCache.clearAll(new HashSet<>(keysToRmv));

            asyncCache.future().get();
        }
        else {
            if (keysToRmv.size() == 1)
                jcache().clear(F.first(keysToRmv));
            else
                jcache().clearAll(new HashSet<>(keysToRmv));
        }

        for (int i = 0; i < 500; ++i) {
            String key = "key" + i;

            boolean found = false;

            for (int j = 0; j < gridCount(); j++) {
                if (jcache(j).localPeek(key) != null)
                    found = true;
            }

            if (!keysToRmv.contains(key))
                assertTrue("Not found key " + key, found);
            else
                assertFalse("Found removed key " + key, found);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void _testWithSkipStore() throws Exception {
        if(isMultiJvm())
            fail("https://issues.apache.org/jira/browse/IGNITE-1088");

        if (!storeEnabled())
            return;

        IgniteCache<String, Integer> cache = grid(0).cache(cacheName());

        IgniteCache<String, Integer> cacheSkipStore = cache.withSkipStore();

        List<String> keys = primaryKeysForCache(cache, 10);

        for (int i = 0; i < keys.size(); ++i)
            putToStore(keys.get(i), i);

        assertFalse(cacheSkipStore.iterator().hasNext());

        for (String key : keys) {
            assertNull(cacheSkipStore.get(key));

            assertNotNull(cache.get(key));
        }

        for (String key : keys) {
            cacheSkipStore.remove(key);

            assertNotNull(cache.get(key));
        }

        cache.removeAll(new HashSet<>(keys));

        for (String key : keys)
            assertNull(cache.get(key));

        final int KEYS = 250;

        // Put/remove data from multiple nodes.

        keys = new ArrayList<>(KEYS);

        for (int i = 0; i < KEYS; i++)
            keys.add("key_" + i);

        for (int i = 0; i < keys.size(); ++i)
            cache.put(keys.get(i), i);

        for (int i = 0; i < keys.size(); ++i) {
            String key = keys.get(i);

            assertNotNull(cacheSkipStore.get(key));
            assertNotNull(cache.get(key));
            assertEquals(i, map.get(key));
        }

        for (int i = 0; i < keys.size(); ++i) {
            String key = keys.get(i);

            Integer val1 = -1;

            cacheSkipStore.put(key, val1);
            assertEquals(i, map.get(key));
            assertEquals(val1, cacheSkipStore.get(key));

            Integer val2 = -2;

            assertEquals(val1, cacheSkipStore.invoke(key, new SetValueProcessor(val2)));
            assertEquals(i, map.get(key));
            assertEquals(val2, cacheSkipStore.get(key));
        }

        for (String key : keys) {
            cacheSkipStore.remove(key);

            assertNull(cacheSkipStore.get(key));
            assertNotNull(cache.get(key));
            assertTrue(map.containsKey(key));
        }

        for (String key : keys) {
            cache.remove(key);

            assertNull(cacheSkipStore.get(key));
            assertNull(cache.get(key));
            assertFalse(map.containsKey(key));

            map.put(key, 0);

            Integer val = -1;

            assertNull(cacheSkipStore.invoke(key, new SetValueProcessor(val)));
            assertEquals(0, map.get(key));
            assertEquals(val, cacheSkipStore.get(key));

            cache.remove(key);

            map.put(key, 0);

            assertTrue(cacheSkipStore.putIfAbsent(key, val));
            assertEquals(val, cacheSkipStore.get(key));
            assertEquals(0, map.get(key));

            cache.remove(key);

            map.put(key, 0);

            assertNull(cacheSkipStore.getAndPut(key, val));
            assertEquals(val, cacheSkipStore.get(key));
            assertEquals(0, map.get(key));

            cache.remove(key);
        }

        assertFalse(cacheSkipStore.iterator().hasNext());
        assertTrue(map.size() == 0);
        assertTrue(cache.size(ALL) == 0);

        // putAll/removeAll from multiple nodes.

        Map<String, Integer> data = new LinkedHashMap<>();

        for (int i = 0; i < keys.size(); i++)
            data.put(keys.get(i), i);

        cacheSkipStore.putAll(data);

        for (String key : keys) {
            assertNotNull(cacheSkipStore.get(key));
            assertNotNull(cache.get(key));
            assertFalse(map.containsKey(key));
        }

        cache.putAll(data);

        for (String key : keys) {
            assertNotNull(cacheSkipStore.get(key));
            assertNotNull(cache.get(key));
            assertTrue(map.containsKey(key));
        }

        cacheSkipStore.removeAll(data.keySet());

        for (String key : keys) {
            assertNull(cacheSkipStore.get(key));
            assertNotNull(cache.get(key));
            assertTrue(map.containsKey(key));
        }

        cacheSkipStore.putAll(data);

        for (String key : keys) {
            assertNotNull(cacheSkipStore.get(key));
            assertNotNull(cache.get(key));
            assertTrue(map.containsKey(key));
        }

        cacheSkipStore.removeAll(data.keySet());

        for (String key : keys) {
            assertNull(cacheSkipStore.get(key));
            assertNotNull(cache.get(key));
            assertTrue(map.containsKey(key));
        }

        cache.removeAll(data.keySet());

        for (String key : keys) {
            assertNull(cacheSkipStore.get(key));
            assertNull(cache.get(key));
            assertFalse(map.containsKey(key));
        }

        assertTrue(map.size() == 0);

        // Miscellaneous checks.

        String newKey = "New key";

        assertFalse(map.containsKey(newKey));

        cacheSkipStore.put(newKey, 1);

        assertFalse(map.containsKey(newKey));

        cache.put(newKey, 1);

        assertTrue(map.containsKey(newKey));

        Iterator<Cache.Entry<String, Integer>> it = cacheSkipStore.iterator();

        assertTrue(it.hasNext());

        Cache.Entry<String, Integer> entry = it.next();

        String rmvKey = entry.getKey();

        assertTrue(map.containsKey(rmvKey));

        it.remove();

        assertNull(cacheSkipStore.get(rmvKey));

        assertTrue(map.containsKey(rmvKey));

        assertTrue(cache.size(ALL) == 0);
        assertTrue(cacheSkipStore.size(ALL) == 0);

        cache.remove(rmvKey);

        assertTrue(map.size() == 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testWithSkipStoreRemoveAll() throws Exception {
        if (isMultiJvm())
            fail("https://issues.apache.org/jira/browse/IGNITE-1088");

        if (atomicityMode() == TRANSACTIONAL || (atomicityMode() == ATOMIC && nearEnabled())) // TODO IGNITE-373.
            return;

        if (!storeEnabled())
            return;

        IgniteCache<String, Integer> cache = grid(0).cache(cacheName());

        IgniteCache<String, Integer> cacheSkipStore = cache.withSkipStore();

        Map<String, Integer> data = new HashMap<>();

        for (int i = 0; i < 100; i++)
            data.put("key_" + i, i);

        cache.putAll(data);

        for (String key : data.keySet()) {
            assertNotNull(cacheSkipStore.get(key));
            assertNotNull(cache.get(key));
            assertTrue(map.containsKey(key));
        }

        cacheSkipStore.removeAll();

        for (String key : data.keySet()) {
            assertNull(cacheSkipStore.get(key));
            assertNotNull(cache.get(key));
            assertTrue(map.containsKey(key));
        }

        cache.removeAll();

        for (String key : data.keySet()) {
            assertNull(cacheSkipStore.get(key));
            assertNull(cache.get(key));
            assertFalse(map.containsKey(key));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void _testWithSkipStoreTx() throws Exception {
        if (txShouldBeUsed() && storeEnabled()) {
            IgniteCache<String, Integer> cache = grid(0).cache(cacheName());

            IgniteCache<String, Integer> cacheSkipStore = cache.withSkipStore();

            final int KEYS = 250;

            // Put/remove data from multiple nodes.

            List<String> keys = new ArrayList<>(KEYS);

            for (int i = 0; i < KEYS; i++)
                keys.add("key_" + i);

            Map<String, Integer> data = new LinkedHashMap<>();

            for (int i = 0; i < keys.size(); i++)
                data.put(keys.get(i), i);

            checkSkipStoreWithTransaction(cache, cacheSkipStore, data, keys, OPTIMISTIC, READ_COMMITTED);

            checkSkipStoreWithTransaction(cache, cacheSkipStore, data, keys, OPTIMISTIC, REPEATABLE_READ);

            checkSkipStoreWithTransaction(cache, cacheSkipStore, data, keys, OPTIMISTIC, SERIALIZABLE);

            checkSkipStoreWithTransaction(cache, cacheSkipStore, data, keys, PESSIMISTIC, READ_COMMITTED);

            checkSkipStoreWithTransaction(cache, cacheSkipStore, data, keys, PESSIMISTIC, REPEATABLE_READ);

            checkSkipStoreWithTransaction(cache, cacheSkipStore, data, keys, PESSIMISTIC, SERIALIZABLE);
        }
    }

    /**
     * @param cache Cache instance.
     * @param cacheSkipStore Cache skip store projection.
     * @param data Data set.
     * @param keys Keys list.
     * @param txConcurrency Concurrency mode.
     * @param txIsolation Isolation mode.
     *
     * @throws Exception If failed.
     */
    private void checkSkipStoreWithTransaction(IgniteCache<String, Integer> cache,
        IgniteCache<String, Integer> cacheSkipStore,
        Map<String, Integer> data,
        List<String> keys,
        TransactionConcurrency txConcurrency,
        TransactionIsolation txIsolation)
        throws  Exception
    {
        info("Test tx skip store [concurrency=" + txConcurrency + ", isolation=" + txIsolation + ']');

        cache.removeAll(data.keySet());
        checkEmpty(cache, cacheSkipStore);

        IgniteTransactions txs = cache.unwrap(Ignite.class).transactions();

        Integer val = -1;

        // Several put check.
        try (Transaction tx = txs.txStart(txConcurrency, txIsolation)) {
            for (String key: keys)
                cacheSkipStore.put(key, val);

            for (String key: keys) {
                assertEquals(val, cacheSkipStore.get(key));
                assertEquals(val, cache.get(key));
                assertFalse(map.containsKey(key));
            }

            tx.commit();
        }

        for (String key: keys) {
            assertEquals(val, cacheSkipStore.get(key));
            assertEquals(val, cache.get(key));
            assertFalse(map.containsKey(key));
        }

        assertEquals(0, map.size());

        // cacheSkipStore putAll(..)/removeAll(..) check.
        try (Transaction tx = txs.txStart(txConcurrency, txIsolation)) {
            cacheSkipStore.putAll(data);

            tx.commit();
        }

        for (String key: keys) {
            val = data.get(key);

            assertEquals(val, cacheSkipStore.get(key));
            assertEquals(val, cache.get(key));
            assertFalse(map.containsKey(key));
        }

        map.putAll(data);

        try (Transaction tx = txs.txStart(txConcurrency, txIsolation)) {
            cacheSkipStore.removeAll(data.keySet());

            tx.commit();
        }

        for (String key: keys) {
            assertNull(cacheSkipStore.get(key));
            assertNotNull(cache.get(key));
            assertTrue(map.containsKey(key));

            cache.remove(key);
        }

        assertTrue(map.size() == 0);

        // cache putAll(..)/removeAll(..) check.
        try (Transaction tx = txs.txStart(txConcurrency, txIsolation)) {
            cache.putAll(data);

            for (String key: keys) {
                assertNotNull(cacheSkipStore.get(key));
                assertNotNull(cache.get(key));
                assertFalse(map.containsKey(key));
            }

            cache.removeAll(data.keySet());

            for (String key: keys) {
                assertNull(cacheSkipStore.get(key));
                assertNull(cache.get(key));
                assertFalse(map.containsKey(key));
            }

            tx.commit();
        }

        assertTrue(map.size() == 0);

        // putAll(..) from both cacheSkipStore and cache.
        try (Transaction tx = txs.txStart(txConcurrency, txIsolation)) {
            Map<String, Integer> subMap = new HashMap<>();

            for (int i = 0; i < keys.size() / 2; i++)
                subMap.put(keys.get(i), i);

            cacheSkipStore.putAll(subMap);

            subMap.clear();

            for (int i = keys.size() / 2; i < keys.size(); i++)
                subMap.put(keys.get(i), i);

            cache.putAll(subMap);

            for (String key: keys) {
                assertNotNull(cacheSkipStore.get(key));
                assertNotNull(cache.get(key));
                assertFalse(map.containsKey(key));
            }

            tx.commit();
        }

        for (int i = 0; i < keys.size() / 2; i++) {
            String key = keys.get(i);

            assertNotNull(cacheSkipStore.get(key));
            assertNotNull(cache.get(key));
            assertFalse(map.containsKey(key));
        }

        for (int i = keys.size() / 2; i < keys.size(); i++) {
            String key = keys.get(i);

            assertNotNull(cacheSkipStore.get(key));
            assertNotNull(cache.get(key));
            assertTrue(map.containsKey(key));
        }

        cache.removeAll(data.keySet());

        for (String key: keys) {
            assertNull(cacheSkipStore.get(key));
            assertNull(cache.get(key));
            assertFalse(map.containsKey(key));
        }

        // Check that read-through is disabled when cacheSkipStore is used.
        for (int i = 0; i < keys.size(); i++)
            putToStore(keys.get(i), i);

        assertTrue(cacheSkipStore.size(ALL) == 0);
        assertTrue(cache.size(ALL) == 0);
        assertTrue(map.size() != 0);

        try (Transaction tx = txs.txStart(txConcurrency, txIsolation)) {
            assertTrue(cacheSkipStore.getAll(data.keySet()).size() == 0);

            for (String key : keys) {
                assertNull(cacheSkipStore.get(key));

                if (txIsolation == READ_COMMITTED) {
                    assertNotNull(cache.get(key));
                    assertNotNull(cacheSkipStore.get(key));
                }
            }

            tx.commit();
        }

        cache.removeAll(data.keySet());

        val = -1;

        try (Transaction tx = txs.txStart(txConcurrency, txIsolation)) {
            for (String key : data.keySet()) {
                map.put(key, 0);

                assertNull(cacheSkipStore.invoke(key, new SetValueProcessor(val)));
            }

            tx.commit();
        }

        for (String key : data.keySet()) {
            assertEquals(0, map.get(key));

            assertEquals(val, cacheSkipStore.get(key));
            assertEquals(val, cache.get(key));
        }

        cache.removeAll(data.keySet());

        try (Transaction tx = txs.txStart(txConcurrency, txIsolation)) {
            for (String key : data.keySet()) {
                map.put(key, 0);

                assertTrue(cacheSkipStore.putIfAbsent(key, val));
            }

            tx.commit();
        }

        for (String key : data.keySet()) {
            assertEquals(0, map.get(key));

            assertEquals(val, cacheSkipStore.get(key));
            assertEquals(val, cache.get(key));
        }

        cache.removeAll(data.keySet());

        try (Transaction tx = txs.txStart(txConcurrency, txIsolation)) {
            for (String key : data.keySet()) {
                map.put(key, 0);

                assertNull(cacheSkipStore.getAndPut(key, val));
            }

            tx.commit();
        }

        for (String key : data.keySet()) {
            assertEquals(0, map.get(key));

            assertEquals(val, cacheSkipStore.get(key));
            assertEquals(val, cache.get(key));
        }

        cache.removeAll(data.keySet());
        checkEmpty(cache, cacheSkipStore);
    }

    /**
     * @param cache Cache instance.
     * @param cacheSkipStore Cache skip store projection.
     *
     * @throws Exception If failed.
     */
    private void checkEmpty(IgniteCache<String, Integer> cache, IgniteCache<String, Integer> cacheSkipStore)
        throws Exception {
        assertTrue(cache.size(ALL) == 0);
        assertTrue(cacheSkipStore.size(ALL) == 0);
        assertTrue(map.size() == 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testGetOutTx() throws Exception {
        checkGetOutTx(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testGetOutTxAsync() throws Exception {
        checkGetOutTx(true);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkGetOutTx(boolean async) throws Exception {
        final AtomicInteger lockEvtCnt = new AtomicInteger();

        IgnitePredicate<Event> lsnr = new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                lockEvtCnt.incrementAndGet();

                return true;
            }
        };

        try {
            IgniteCache<String, Integer> cache = grid(0).cache(cacheName());

            List<String> keys = primaryKeysForCache(cache, 2);

            assertEquals(2, keys.size());

            cache.put(keys.get(0), 0);
            cache.put(keys.get(1), 1);

            grid(0).events().localListen(lsnr, EVT_CACHE_OBJECT_LOCKED, EVT_CACHE_OBJECT_UNLOCKED);

            if (async)
                cache = cache.withAsync();

            try (Transaction tx = transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                Integer val0 = cache.get(keys.get(0));

                if (async)
                    val0 = cache.<Integer>future().get();

                assertEquals(0, val0.intValue());

                Map<String, Integer> allOutTx = cache.getAllOutTx(F.asSet(keys.get(1)));

                if (async)
                    allOutTx = cache.<Map<String, Integer>>future().get();

                assertEquals(1, allOutTx.size());

                assertTrue(allOutTx.containsKey(keys.get(1)));

                assertEquals(1, allOutTx.get(keys.get(1)).intValue());
            }

            assertTrue(GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    info("Lock event count: " + lockEvtCnt.get());
                    if (atomicityMode() == ATOMIC)
                        return lockEvtCnt.get() == 0;

                    if (cacheMode() == PARTITIONED && nearEnabled()) {
                        if (!grid(0).configuration().isClientMode())
                            return lockEvtCnt.get() == 4;
                    }

                    return lockEvtCnt.get() == 2;
                }
            }, 15000));
        }
        finally {
            grid(0).events().stopLocalListen(lsnr, EVT_CACHE_OBJECT_LOCKED, EVT_CACHE_OBJECT_UNLOCKED);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void _testInvokeException() throws Exception {
        final IgniteCache cache = jcache().withAsync();

        cache.invoke("key2", ERR_PROCESSOR);

        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgniteFuture fut = cache.future().chain(new IgniteClosure<IgniteFuture, Object>() {
                    @Override public Object apply(IgniteFuture o) {
                        return o.get();
                    }
                });

                fut.get();

                return null;
            }
        }, EntryProcessorException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testLockInsideTransaction() throws Exception {
        if (txEnabled()) {
            GridTestUtils.assertThrows(
                log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        try (Transaction tx = ignite(0).transactions().txStart()) {
                            jcache(0).lock("key").lock();
                        }

                        return null;
                    }
                },
                CacheException.class,
                "Explicit lock can't be acquired within a transaction."
            );

            GridTestUtils.assertThrows(
                log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        try (Transaction tx = ignite(0).transactions().txStart()) {
                            jcache(0).lockAll(Arrays.asList("key1", "key2")).lock();
                        }

                        return null;
                    }
                },
                CacheException.class,
                "Explicit lock can't be acquired within a transaction."
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void _testContinuousQuery1() throws Exception {
        checkContinuousQuery(DataMode.PLANE_OBJECT);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testContinuousQuery2() throws Exception {
        checkContinuousQuery(DataMode.SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testContinuousQuery3() throws Exception {
        checkContinuousQuery(DataMode.EXTERNALIZABLE);
    }

    /**
     * @param mode Data mode.
     * @throws IgniteInterruptedCheckedException
     */
    @SuppressWarnings("serial")
    private void checkContinuousQuery(DataMode mode) throws IgniteInterruptedCheckedException {
        final AtomicInteger updCnt = new AtomicInteger();

        ContinuousQuery<TestObject, TestObject> qry = new ContinuousQuery<>();

        qry.setInitialQuery(new ScanQuery<>(new IgniteBiPredicate<TestObject, TestObject>() {
            @Override public boolean apply(TestObject key, TestObject val) {
                return key.value() >= 3;
            }
        }));

        qry.setLocalListener(new CacheEntryUpdatedListener<TestObject, TestObject>() {
            @Override public void onUpdated(
                Iterable<CacheEntryEvent<? extends TestObject, ? extends TestObject>> evts) throws CacheEntryListenerException {
                for (CacheEntryEvent<? extends TestObject, ? extends TestObject> evt : evts) {
                    int v = evt.getKey().value();

                    // Check filter.
                    assertTrue("v=" + v, v >= 10 && v < 15);

                    updCnt.incrementAndGet();
                }
            }
        });

        qry.setRemoteFilter(new TestCacheEntryEventSerializableFilter());

        IgniteCache<TestObject, TestObject> cache = jcache();

        for (int i = 0; i < 10; i++)
            cache.put(key(i, mode), value(i, mode));

        try (QueryCursor<Cache.Entry<TestObject, TestObject>> cur = cache.query(qry)) {
            int cnt = 0;

            for (Cache.Entry<TestObject, TestObject> e : cur) {
                cnt++;

                int val = e.getKey().value();

                assertTrue("v=" + val, val >= 3);
            }

            assertEquals(7, cnt);

            for (int i = 10; i < 20; i++)
                cache.put(key(i, mode), value(i, mode));

            GridTestUtils.waitForCondition(new GridAbsPredicateX() {
                @Override public boolean applyx() throws IgniteCheckedException {
                    return updCnt.get() == 5;
                }
            }, 30_000);
        }
    }

    /**
     * @param keyId Key Id..
     * @param mode Mode.
     * @return Key.
     */
    public static TestObject key(int keyId, DataMode mode) {
        switch (mode) {
            case SERIALIZABLE:
                return new SerializableObject(keyId);
            case EXTERNALIZABLE:
                return new ExternalizableObject(keyId);
            case PLANE_OBJECT:
                return new TestObject(keyId);
            default:
                throw new IllegalArgumentException("mode: " + mode);
        }
    }

    /**
     * @param idx Index.
     * @param mode Mode.
     * @return Value.
     */
    public static TestObject value(int idx, DataMode mode) {
        switch (mode) {
            case SERIALIZABLE:
                return new SerializableObject(idx);
            case EXTERNALIZABLE:
                return new ExternalizableObject(idx);
            case PLANE_OBJECT:
                return new TestObject(idx);
            default:
                throw new IllegalArgumentException("mode: " + mode);
        }
    }

    /**
     * Sets given value, returns old value.
     */
    public static final class SetValueProcessor implements EntryProcessor<String, Integer, Integer> {
        /** */
        private Integer newVal;

        /**
         * @param newVal New value to set.
         */
        SetValueProcessor(Integer newVal) {
            this.newVal = newVal;
        }

        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<String, Integer> entry, Object... arguments) throws EntryProcessorException {
            Integer val = entry.getValue();

            entry.setValue(newVal);

            return val;
        }
    }

    /**
     *
     */
    public enum CacheStartMode {
        /** Start caches together nodes (not dynamically) */
        STATIC,

        /** */
        NODES_THEN_CACHES,

        /** */
        ONE_BY_ONE
    }

    /**
     *
     */
    private static class RemoveEntryProcessor implements EntryProcessor<TestObject, TestObject, TestObject>, Serializable {
        /** {@inheritDoc} */
        @Override public TestObject process(MutableEntry<TestObject, TestObject> e, Object... args) {
            assertNotNull(e.getKey());

            TestObject old = e.getValue();

            e.remove();

            return old;
        }
    }

    /**
     *
     */
    private static class IncrementEntryProcessor implements EntryProcessor<TestObject, TestObject, TestObject>, Serializable {
        /** {@inheritDoc} */
        @Override public TestObject process(MutableEntry<TestObject, TestObject> e, Object... args) {
            assert !F.isEmpty(args);

            DataMode mode = (DataMode)args[0];

            assertNotNull(e.getKey());

            TestObject old = e.getValue();

            e.setValue(old == null ? value(1, mode) : value(old.value() + 1, mode));

            return old;
        }
    }

    /**
     *
     */
    private static class CheckEntriesTask extends TestIgniteIdxRunnable {
        /** Keys. */
        private final Collection<String> keys;

        /** */
        private String cacheName;

        /**
         * @param keys Keys.
         * @param s
         */
        public CheckEntriesTask(Collection<String> keys, String s) {
            this.keys = keys;
            cacheName = s;
        }

        /** {@inheritDoc} */
        @Override public void run(int idx) throws Exception {
            GridCacheContext<String, Integer> ctx = ((IgniteKernal)ignite).<String, Integer>internalCache(cacheName).context();

            if (ctx.cache().configuration().getMemoryMode() == OFFHEAP_TIERED)
                return;

            int size = 0;

            for (String key : keys) {
                if (ctx.affinity().localNode(key, ctx.discovery().topologyVersionEx())) {
                    GridCacheEntryEx e =
                        ctx.isNear() ? ctx.near().dht().peekEx(key) : ctx.cache().peekEx(key);

                    assert e != null : "Entry is null [idx=" + idx + ", key=" + key + ", ctx=" + ctx + ']';
                    assert !e.deleted() : "Entry is deleted: " + e;

                    size++;
                }
            }

            assertEquals("Incorrect size on cache #" + idx, size, ignite.cache(ctx.name()).localSize(ALL));
        }
    }

    /**
     *
     */
    private static class CheckCacheSizeTask extends TestIgniteIdxRunnable {
        /** */
        private final Map<String, Integer> map;

        /** */
        private String cacheName;

        /**
         * @param map Map.
         */
        public CheckCacheSizeTask(Map<String, Integer> map, String cacheName) {
            this.map = map;

            this.cacheName = cacheName;
        }

        /** {@inheritDoc} */
        @Override public void run(int idx) throws Exception {
            GridCacheContext<String, Integer> ctx = ((IgniteKernal)ignite).<String, Integer>internalCache(cacheName).context();

            int size = 0;

            for (String key : map.keySet())
                if (ctx.affinity().localNode(key, ctx.discovery().topologyVersionEx()))
                    size++;

            assertEquals("Incorrect key size on cache #" + idx, size, ignite.cache(ctx.name()).localSize(ALL));
        }
    }

    /**
     *
     */
    private static class CheckPrimaryKeysTask implements TestCacheCallable<String, Integer, List<String>> {
        /** Start from. */
        private final int startFrom;

        /** Count. */
        private final int cnt;

        /**
         * @param startFrom Start from.
         * @param cnt Count.
         */
        public CheckPrimaryKeysTask(int startFrom, int cnt) {
            this.startFrom = startFrom;
            this.cnt = cnt;
        }

        /** {@inheritDoc} */
        @Override public List<String> call(Ignite ignite, IgniteCache<String, Integer> cache) throws Exception {
            List<String> found = new ArrayList<>();

            Affinity<Object> affinity = ignite.affinity(cache.getName());

            for (int i = startFrom; i < startFrom + 100_000; i++) {
                String key = "key" + i;

                if (affinity.isPrimary(ignite.cluster().localNode(), key)) {
                    found.add(key);

                    if (found.size() == cnt)
                        return found;
                }
            }

            throw new IgniteException("Unable to find " + cnt + " keys as primary for cache.");
        }
    }

    /**
     *
     */
    private static class CheckPrimaryTestObjectKeysTask implements TestCacheCallable<TestObject, TestObject, List<TestObject>> {
        /** Start from. */
        private final int startFrom;

        /** Count. */
        private final int cnt;

        /** */
        private final DataMode mode;

        /**
         * @param startFrom Start from.
         * @param cnt Count.
         */
        public CheckPrimaryTestObjectKeysTask(int startFrom, int cnt, DataMode mode) {
            this.startFrom = startFrom;
            this.cnt = cnt;
            this.mode = mode;
        }

        /** {@inheritDoc} */
        @Override public List<TestObject> call(Ignite ignite, IgniteCache<TestObject, TestObject> cache) throws Exception {
            List<TestObject> found = new ArrayList<>();

            Affinity<TestObject> affinity = ignite.affinity(cache.getName());

            for (int i = startFrom; i < startFrom + 100_000; i++) {
                TestObject key = key(i, mode);

                if (affinity.isPrimary(ignite.cluster().localNode(), key)) {
                    found.add(key);

                    if (found.size() == cnt)
                        return found;
                }
            }

            throw new IgniteException("Unable to find " + cnt + " keys as primary for cache.");
        }
    }

    /**
     *
     */
    private static class CheckIteratorTask extends TestIgniteIdxCallable<Void> {
        /** */
        private String cacheName;

        /**
         * @param cacheName Name.
         */
        public CheckIteratorTask(String cacheName) {
            this.cacheName = cacheName;
        }

        /**
         * @param idx Index.
         */
        @Override public Void call(int idx) throws Exception {
            GridCacheContext<String, Integer> ctx = ((IgniteKernal)ignite).<String, Integer>internalCache(cacheName).context();
            GridCacheQueryManager queries = ctx.queries();

            Map map = GridTestUtils.getFieldValue(queries, GridCacheQueryManager.class, "qryIters");

            for (Object obj : map.values())
                assertEquals("Iterators not removed for grid " + idx, 0, ((Map)obj).size());

            return null;
        }
    }

    /**
     *
     */
    private static class RemoveAndReturnNullEntryProcessor implements
        EntryProcessor<TestObject, TestObject, TestObject>, Serializable {

        /** {@inheritDoc} */
        @Override public TestObject process(MutableEntry<TestObject, TestObject> e, Object... args) {
            e.remove();

            return null;
        }
    }

    /**
     *
     */
    private static class SwapEvtsLocalListener implements IgnitePredicate<Event> {
        @LoggerResource
        private IgniteLogger log;

        /** Swap events. */
        private final AtomicInteger swapEvts;

        /** Unswap events. */
        private final AtomicInteger unswapEvts;

        /**
         * @param swapEvts Swap events.
         * @param unswapEvts Unswap events.
         */
        public SwapEvtsLocalListener(AtomicInteger swapEvts, AtomicInteger unswapEvts) {
            this.swapEvts = swapEvts;
            this.unswapEvts = unswapEvts;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(Event evt) {
            log.info("Received event: " + evt);

            switch (evt.type()) {
                case EVT_CACHE_OBJECT_SWAPPED:
                    swapEvts.incrementAndGet();

                    break;
                case EVT_CACHE_OBJECT_UNSWAPPED:
                    unswapEvts.incrementAndGet();

                    break;
            }

            return true;
        }
    }

    private static class CheckEntriesDeletedTask extends TestIgniteIdxRunnable {
        private final int cnt;

        /** */
        private String cacheName;

        public CheckEntriesDeletedTask(int cnt, String cacheName) {
            this.cnt = cnt;
            this.cacheName = cacheName;
        }

        @Override public void run(int idx) throws Exception {
            for (int i = 0; i < cnt; i++) {
                String key = String.valueOf(i);

                GridCacheContext<String, Integer> ctx = ((IgniteKernal)ignite).<String, Integer>internalCache(cacheName).context();

                GridCacheEntryEx entry = ctx.isNear() ? ctx.near().dht().peekEx(key) : ctx.cache().peekEx(key);

                if (ignite.affinity(cacheName).mapKeyToPrimaryAndBackups(key).contains(((IgniteKernal)ignite).localNode())) {
                    assertNotNull(entry);
                    assertTrue(entry.deleted());
                }
                else
                    assertNull(entry);
            }
        }
    }

    /**
     *
     */
    private static class CheckKeySizeTask extends TestIgniteIdxRunnable {
        /** Keys. */
        private final Collection<String> keys;

        /** */
        private String cacheName;

        /**
         * @param keys Keys.
         * @param s
         */
        public CheckKeySizeTask(Collection<String> keys, String s) {
            this.keys = keys;
            this.cacheName = s;
        }

        /** {@inheritDoc} */
        @Override public void run(int idx) throws Exception {
            GridCacheContext<String, Integer> ctx = ((IgniteKernal)ignite).<String, Integer>internalCache(cacheName).context();

            int size = 0;

            for (String key : keys)
                if (ctx.affinity().localNode(key, ctx.discovery().topologyVersionEx()))
                    size++;

            assertEquals("Incorrect key size on cache #" + idx, size, ignite.cache(cacheName).localSize(ALL));
        }
    }

    /**
     *
     */
    private static class FailedEntryProcessor implements EntryProcessor<TestObject, TestObject, TestObject>, Serializable {
        /** {@inheritDoc} */
        @Override public TestObject process(MutableEntry<TestObject, TestObject> e, Object... args) {
            throw new EntryProcessorException("Test entry processor exception.");
        }
    }

    /**
     *
     */
    public static class TestObject {
        /** */
        protected int val;

        /** */
        protected String strVal;

        /** */
        protected TestEnum enumVal;

        /**
         * @param val Value.
         */
        TestObject(int val) {
            this.val = val;
            strVal = "val" + val;

            TestEnum[] values = TestEnum.values();
            enumVal = values[Math.abs(val) % values.length];
        }

        /**
         * @return Value.
         */
        public int value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof TestObject))
                return false;

            TestObject val = (TestObject)o;

            return this.val == val.val && enumVal == val.enumVal && strVal.equals(val.strVal);

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = val;

            res = 31 * res + strVal.hashCode();
            res = 31 * res + enumVal.hashCode();

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return getClass().getSimpleName() + "[" +
                "val=" + val +
                ", strVal='" + strVal + '\'' +
                ", enumVal=" + enumVal +
                ']';
        }
    }

    /**
     *
     */
    private static class SerializableObject extends TestObject implements Serializable {
        /**
         * @param val Value.
         */
        SerializableObject(int val) {
            super(val);
        }
    }

    /**
     *
     */
    private static class ExternalizableObject extends TestObject implements Externalizable {
        /**
         * Default constructor.
         */
        ExternalizableObject() {
            super(-1);
        }

        /**
         * @param val Value.
         */
        ExternalizableObject(int val) {
            super(val);
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(val);
            out.writeObject(strVal);
            out.writeObject(enumVal);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            val = in.readInt();
            strVal = (String)in.readObject();
            enumVal = (TestEnum)in.readObject();
        }
    }

    /**
     * Data mode.
     */
    public enum DataMode {
        /** Serializable objects. */
        SERIALIZABLE,

        /** Externalizable objects. */
        EXTERNALIZABLE,

        /** Objects without Serializable and Externalizable. */
        PLANE_OBJECT
    }

    /**
     *
     */
    private enum TestEnum {
        /** */
        TEST_VALUE_1,

        /** */
        TEST_VALUE_2,

        /** */
        TEST_VALUE_3
    }

    /**
     *
     */
    private static class TestCacheEntryEventSerializableFilter implements CacheEntryEventSerializableFilter<TestObject, TestObject> {
        /** {@inheritDoc} */
        @Override public boolean evaluate(
            CacheEntryEvent<? extends TestObject, ? extends TestObject> evt) throws CacheEntryListenerException {
            return evt.getKey().value() < 15;
        }
    }

}
