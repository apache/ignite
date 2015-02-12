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

import com.google.common.collect.*;
import junit.framework.*;
import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.swapspace.inmemory.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.expiry.*;
import javax.cache.processor.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.events.EventType.*;
import static org.apache.ignite.internal.processors.cache.GridCachePeekMode.*;
import static org.apache.ignite.testframework.GridTestUtils.*;
import static org.apache.ignite.transactions.IgniteTxConcurrency.*;
import static org.apache.ignite.transactions.IgniteTxIsolation.*;
import static org.apache.ignite.transactions.IgniteTxState.*;

/**
 * Full API cache test.
 */
public abstract class GridCacheAbstractFullApiSelfTest extends GridCacheAbstractSelfTest {
    /** Increment processor for invoke operations. */
    public static final EntryProcessor<String, Integer, String> INCR_PROCESSOR = new EntryProcessor<String, Integer, String>() {
        @Override public String process(MutableEntry<String, Integer> e, Object... args) {
            assertNotNull(e.getKey());

            Integer old = e.getValue();

            e.setValue(old == null ? 1 : old + 1);

            return String.valueOf(old);
        }
    };

    /** Increment processor for invoke operations. */
    public static final EntryProcessor<String, Integer, String> RMV_PROCESSOR = new EntryProcessor<String, Integer, String>() {
        @Override public String process(MutableEntry<String, Integer> e, Object... args) {
            assertNotNull(e.getKey());

            Integer old = e.getValue();

            e.remove();

            return String.valueOf(old);
        }
    };

    /** Dflt grid. */
    protected Ignite dfltIgnite;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected boolean swapEnabled() {
        return true;
    }

    /**
     * @return {@code True} if values should be stored off-heap.
     */
    protected boolean offHeapValues() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (offHeapValues())
            cfg.setSwapSpaceSpi(new GridTestSwapSpaceSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(gridName);

        if (offHeapValues()) {
            ccfg.setQueryIndexEnabled(false);
            ccfg.setMemoryMode(CacheMemoryMode.OFFHEAP_VALUES);
            ccfg.setOffHeapMaxMemory(0);
        }

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        for (int i = 0; i < gridCount(); i++)
            info("Grid " + i + ": " + grid(i).localNode().id());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        assertEquals(0, cache.localSize());
        assertEquals(0, cache.size());

        super.beforeTest();

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

        Map<String, Integer> map = new HashMap<>();

        for (int i = 0; i < size; i++)
            map.put("key" + i, i);

        // Put in primary nodes to avoid near readers which will prevent entry from being cleared.
        Map<ClusterNode, Collection<String>> mapped = grid(0).mapKeysToNodes(null, map.keySet());

        for (int i = 0; i < gridCount(); i++) {
            Collection<String> keys = mapped.get(grid(i).localNode());

            if (!F.isEmpty(keys)) {
                for (String key : keys)
                    jcache(i).put(key, map.get(key));
            }
        }

        map.remove("key0");

        mapped = grid(0).mapKeysToNodes(null, map.keySet());

        for (int i = 0; i < gridCount(); i++) {
            // Will actually delete entry from map.
            CU.invalidate(cache(i), "key0");

            assertNull("Failed check for grid: " + i, jcache(i).localPeek("key0", CachePeekMode.ONHEAP));

            Collection<String> keysCol = mapped.get(grid(i).localNode());

            assert jcache(i).localSize() != 0 || F.isEmpty(keysCol);
        }

        for (int i = 0; i < gridCount(); i++) {
            GridCacheContext<String, Integer> ctx = context(i);

            int sum = 0;

            for (String key : map.keySet())
                if (ctx.affinity().localNode(key, ctx.discovery().topologyVersion()))
                    sum++;

            assertEquals("Incorrect key size on cache #" + i, sum, jcache(i).localSize());
        }

        for (int i = 0; i < gridCount(); i++) {
            Collection<String> keysCol = mapped.get(grid(i).localNode());

            assertEquals("Failed check for grid: " + i, !F.isEmpty(keysCol) ? keysCol.size() : 0,
                cache(i).primarySize());
        }

        int globalPrimarySize = map.size();

        for (int i = 0; i < gridCount(); i++)
            assertEquals(globalPrimarySize, cache(i).globalPrimarySize());

        int times = 1;

        if (cacheMode() == REPLICATED)
            times = gridCount();
        else if (cacheMode() == PARTITIONED)
            times = Math.min(gridCount(), jcache().getConfiguration(CacheConfiguration.class).getBackups() + 1);

        int globalSize = globalPrimarySize * times;

        for (int i = 0; i < gridCount(); i++)
            assertEquals(globalSize, cache(i).globalSize());
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
    public void testGet() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key1", 1);
        cache.put("key2", 2);

        assert cache.get("key1") == 1;
        assert cache.get("key2") == 2;
        assert cache.get("wrongKey") == null;
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
        IgniteTx tx = txEnabled() ? transactions().txStart() : null;

        final IgniteCache<String, Integer> cache = jcache();

        cache.put("key1", 1);
        cache.put("key2", 2);

        if (tx != null)
            tx.commit();

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
        if (txEnabled()) {
            tx = transactions().txStart();

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

            tx.commit();
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
        if (txEnabled()) {
            try (IgniteTx ignored = transactions().txStart()) {
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
    public void testPut() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        assert cache.getAndPut("key1", 1) == null;
        assert cache.getAndPut("key2", 2) == null;

        // Check inside transaction.
        assert cache.get("key1") == 1;
        assert cache.get("key2") == 2;

        // Put again to check returned values.
        assert cache.getAndPut("key1", 1) == 1;
        assert cache.getAndPut("key2", 2) == 2;

        checkContainsKey(true, "key1");
        checkContainsKey(true, "key2");

        assert cache.get("key1") != null;
        assert cache.get("key2") != null;
        assert cache.get("wrong") == null;

        // Check outside transaction.
        checkContainsKey(true, "key1");
        checkContainsKey(true, "key2");

        assert cache.get("key1") == 1;
        assert cache.get("key2") == 2;
        assert cache.get("wrong") == null;

        assertEquals((Integer)1, cache.getAndPut("key1", 10));
        assertEquals((Integer)2, cache.getAndPut("key2", 11));
    }

    /**
     * @throws Exception In case of error.
     */
    public void testPutTx() throws Exception {
        if (txEnabled()) {
            IgniteTx tx = transactions().txStart();

            IgniteCache<String, Integer> cache = jcache();

            assert cache.getAndPut("key1", 1) == null;
            assert cache.getAndPut("key2", 2) == null;

            // Check inside transaction.
            assert cache.get("key1") == 1;
            assert cache.get("key2") == 2;

            // Put again to check returned values.
            assert cache.getAndPut("key1", 1) == 1;
            assert cache.getAndPut("key2", 2) == 2;

            checkContainsKey(true, "key1");
            checkContainsKey(true, "key2");

            assert cache.get("key1") != null;
            assert cache.get("key2") != null;
            assert cache.get("wrong") == null;

            tx.commit();

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
    public void testTransformOptimisticReadCommitted() throws Exception {
        checkTransform(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformOptimisticRepeatableRead() throws Exception {
        checkTransform(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformPessimisticReadCommitted() throws Exception {
        checkTransform(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformPessimisticRepeatableRead() throws Exception {
        checkTransform(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws Exception If failed.
     */
    private void checkTransform(IgniteTxConcurrency concurrency, IgniteTxIsolation isolation) throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key2", 1);
        cache.put("key3", 3);

        IgniteTx tx = txEnabled() ? ignite(0).transactions().txStart(concurrency, isolation) : null;

        try {
            assertEquals("null", cache.invoke("key1", INCR_PROCESSOR));
            assertEquals("1", cache.invoke("key2", INCR_PROCESSOR));
            assertEquals("3", cache.invoke("key3", RMV_PROCESSOR));

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

        assertEquals((Integer)1, cache.get("key1"));
        assertEquals((Integer)2, cache.get("key2"));
        assertNull(cache.get("key3"));

        for (int i = 0; i < gridCount(); i++)
            assertNull("Failed for cache: " + i, jcache(i).localPeek("key3", CachePeekMode.ONHEAP));

        cache.remove("key1");
        cache.put("key2", 1);
        cache.put("key3", 3);

        assertEquals("null", cache.invoke("key1", INCR_PROCESSOR));
        assertEquals("1", cache.invoke("key2", INCR_PROCESSOR));
        assertEquals("3", cache.invoke("key3", RMV_PROCESSOR));

        assertEquals((Integer)1, cache.get("key1"));
        assertEquals((Integer)2, cache.get("key2"));
        assertNull(cache.get("key3"));

        for (int i = 0; i < gridCount(); i++)
            assertNull(jcache(i).localPeek("key3", CachePeekMode.ONHEAP));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformAllOptimisticReadCommitted() throws Exception {
        checkTransformAll(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformAllOptimisticRepeatableRead() throws Exception {
        checkTransformAll(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformAllPessimisticReadCommitted() throws Exception {
        checkTransformAll(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformAllPessimisticRepeatableRead() throws Exception {
        checkTransformAll(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @param concurrency Transaction concurrency.
     * @param isolation Transaction isolation.
     * @throws Exception If failed.
     */
    private void checkTransformAll(IgniteTxConcurrency concurrency, IgniteTxIsolation isolation)
        throws Exception {
        final IgniteCache<String, Integer> cache = jcache();

        cache.put("key2", 1);
        cache.put("key3", 3);

        if (txEnabled()) {
            Map<String, EntryProcessorResult<String>> res;

            try (IgniteTx tx = ignite(0).transactions().txStart(concurrency, isolation)) {
                res = cache.invokeAll(F.asSet("key1", "key2", "key3"), INCR_PROCESSOR);

                tx.commit();
            }

            assertEquals((Integer)1, cache.get("key1"));
            assertEquals((Integer)2, cache.get("key2"));
            assertEquals((Integer)4, cache.get("key3"));

            assertEquals("null", res.get("key1").get());
            assertEquals("1", res.get("key2").get());
            assertEquals("3", res.get("key3").get());

            assertEquals(3, res.size());

            cache.remove("key1");
            cache.put("key2", 1);
            cache.put("key3", 3);
        }

        Map<String, EntryProcessorResult<String>> res = cache.invokeAll(F.asSet("key1", "key2", "key3"), RMV_PROCESSOR);

        for (int i = 0; i < gridCount(); i++) {
            assertNull(jcache(i).localPeek("key1", CachePeekMode.ONHEAP));
            assertNull(jcache(i).localPeek("key2", CachePeekMode.ONHEAP));
            assertNull(jcache(i).localPeek("key3", CachePeekMode.ONHEAP));
        }

        assertEquals("null", res.get("key1").get());
        assertEquals("1", res.get("key2").get());
        assertEquals("3", res.get("key3").get());

        assertEquals(3, res.size());

        cache.remove("key1");
        cache.put("key2", 1);
        cache.put("key3", 3);

        res = cache.invokeAll(F.asSet("key1", "key2", "key3"), INCR_PROCESSOR);

        assertEquals((Integer)1, cache.get("key1"));
        assertEquals((Integer)2, cache.get("key2"));
        assertEquals((Integer)4, cache.get("key3"));

        assertEquals("null", res.get("key1").get());
        assertEquals("1", res.get("key2").get());
        assertEquals("3", res.get("key3").get());

        assertEquals(3, res.size());

        cache.remove("key1");
        cache.put("key2", 1);
        cache.put("key3", 3);

        res = cache.invokeAll(F.asMap("key1", INCR_PROCESSOR, "key2", INCR_PROCESSOR, "key3", INCR_PROCESSOR));

        assertEquals((Integer)1, cache.get("key1"));
        assertEquals((Integer)2, cache.get("key2"));
        assertEquals((Integer)4, cache.get("key3"));

        assertEquals("null", res.get("key1").get());
        assertEquals("1", res.get("key2").get());
        assertEquals("3", res.get("key3").get());

        assertEquals(3, res.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformAllWithNulls() throws Exception {
        final IgniteCache<String, Integer> cache = jcache();

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.invokeAll((Set<String>)null, INCR_PROCESSOR);

                return null;
            }
        }, NullPointerException.class, null);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.invokeAll(F.asSet("key1"), null);

                return null;
            }
        }, NullPointerException.class, null);

        {
            final Set<String> keys = new LinkedHashSet<>(2);

            keys.add("key1");
            keys.add(null);

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    cache.invokeAll(keys, INCR_PROCESSOR);

                    return null;
                }
            }, NullPointerException.class, null);

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    cache.invokeAll(F.asSet("key1"), null);

                    return null;
                }
            }, NullPointerException.class, null);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformSequentialOptimisticNoStart() throws Exception {
        checkTransformSequential0(false, OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformSequentialPessimisticNoStart() throws Exception {
        checkTransformSequential0(false, PESSIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformSequentialOptimisticWithStart() throws Exception {
        checkTransformSequential0(true, OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformSequentialPessimisticWithStart() throws Exception {
        checkTransformSequential0(true, PESSIMISTIC);
    }

    /**
     * @param startVal Whether to put value.
     * @param concurrency Concurrency.
     * @throws Exception If failed.
     */
    private void checkTransformSequential0(boolean startVal, IgniteTxConcurrency concurrency)
        throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        IgniteTx tx = txEnabled() ? ignite(0).transactions().txStart(concurrency, READ_COMMITTED) : null;

        try {
            if (startVal)
                cache.put("key", 2);

            cache.invoke("key", INCR_PROCESSOR);
            cache.invoke("key", INCR_PROCESSOR);
            cache.invoke("key", INCR_PROCESSOR);

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        Integer exp = (startVal ? 2 : 0) + 3;

        assertEquals(exp, cache.get("key"));

        for (int i = 0; i < gridCount(); i++) {
            if (ignite(i).affinity(null).isPrimaryOrBackup(grid(i).localNode(), "key"))
                assertEquals(exp, peek(jcache(i), "key"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformAfterRemoveOptimistic() throws Exception {
        checkTransformAfterRemove(OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformAfterRemovePessimistic() throws Exception {
        checkTransformAfterRemove(PESSIMISTIC);
    }

    /**
     * @param concurrency Concurrency.
     * @throws Exception If failed.
     */
    private void checkTransformAfterRemove(IgniteTxConcurrency concurrency) throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key", 4);

        IgniteTx tx = txEnabled() ? ignite(0).transactions().txStart(concurrency, READ_COMMITTED) : null;

        try {
            cache.remove("key");

            cache.invoke("key", INCR_PROCESSOR);
            cache.invoke("key", INCR_PROCESSOR);
            cache.invoke("key", INCR_PROCESSOR);

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        assertEquals((Integer)3, cache.get("key"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformReturnValueGetOptimisticReadCommitted() throws Exception {
        checkTransformReturnValue(false, OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformReturnValueGetOptimisticRepeatableRead() throws Exception {
        checkTransformReturnValue(false, OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformReturnValueGetPessimisticReadCommitted() throws Exception {
        checkTransformReturnValue(false, PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformReturnValueGetPessimisticRepeatableRead() throws Exception {
        checkTransformReturnValue(false, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformReturnValuePutInTx() throws Exception {
        checkTransformReturnValue(true, OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @param put Whether to put value.
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws Exception If failed.
     */
    private void checkTransformReturnValue(boolean put,
        IgniteTxConcurrency concurrency,
        IgniteTxIsolation isolation)
        throws Exception
    {
        IgniteCache<String, Integer> cache = jcache();

        if (!put)
            cache.put("key", 1);

        IgniteTx tx = txEnabled() ? ignite(0).transactions().txStart(concurrency, isolation) : null;

        try {
            if (put)
                cache.put("key", 1);

            cache.invoke("key", INCR_PROCESSOR);

            assertEquals((Integer)2, cache.get("key"));

            if (tx != null) {
                // Second get inside tx. Make sure read value is not transformed twice.
                assertEquals((Integer)2, cache.get("key"));

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
    public void testGetAndPutAsync() throws Exception {
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
    public void testPutAsync0() throws Exception {
        IgniteCache<String, Integer> cacheAsync = jcache().withAsync();

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
    public void testInvokeAsync() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key2", 1);
        cache.put("key3", 3);

        IgniteCache<String, Integer> cacheAsync = cache.withAsync();

        assertNull(cacheAsync.invoke("key1", INCR_PROCESSOR));

        IgniteFuture<?> fut0 = cacheAsync.future();

        assertNull(cacheAsync.invoke("key2", INCR_PROCESSOR));

        IgniteFuture<?> fut1 = cacheAsync.future();

        assertNull(cacheAsync.invoke("key3", RMV_PROCESSOR));

        IgniteFuture<?> fut2 = cacheAsync.future();

        fut0.get();
        fut1.get();
        fut2.get();

        assertEquals((Integer)1, cache.get("key1"));
        assertEquals((Integer)2, cache.get("key2"));
        assertNull(cache.get("key3"));

        for (int i = 0; i < gridCount(); i++)
            assertNull(jcache(i).localPeek("key3", CachePeekMode.ONHEAP));
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvoke() throws Exception {
        final IgniteCache<String, Integer> cache = jcache();

        assertEquals("null", cache.invoke("k0", INCR_PROCESSOR));

        assertEquals((Integer)1, cache.get("k0"));

        assertEquals("1", cache.invoke("k0", INCR_PROCESSOR));

        assertEquals((Integer)2, cache.get("k0"));

        cache.put("k1", 1);

        assertEquals("1", cache.invoke("k1", INCR_PROCESSOR));

        assertEquals((Integer)2, cache.get("k1"));

        assertEquals("2", cache.invoke("k1", INCR_PROCESSOR));

        assertEquals((Integer)3, cache.get("k1"));

        EntryProcessor<String, Integer, Integer> c = new EntryProcessor<String, Integer, Integer>() {
            @Override public Integer process(MutableEntry<String, Integer> e, Object... args) {
                e.remove();

                return null;
            }
        };

        assertNull(cache.invoke("k1", c));
        assertNull(cache.get("k1"));

        for (int i = 0; i < gridCount(); i++)
            assertNull(jcache(i).localPeek("k1", CachePeekMode.ONHEAP));

        final EntryProcessor<String, Integer, Integer> errProcessor = new EntryProcessor<String, Integer, Integer>() {
            @Override public Integer process(MutableEntry<String, Integer> e, Object... args) {
                throw new EntryProcessorException("Test entry processor exception.");
            }
        };

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.invoke("k1", errProcessor);

                return null;
            }
        }, EntryProcessorException.class, "Test entry processor exception.");
    }

    /**
     * @throws Exception In case of error.
     */
    public void testPutx() throws Exception {
        if (txEnabled())
            checkPut(true);
    }

    /**
     * @throws Exception In case of error.
     */
    public void testPutxNoTx() throws Exception {
        checkPut(false);
    }

    /**
     * @param inTx Whether to start transaction.
     * @throws Exception If failed.
     */
    private void checkPut(boolean inTx) throws Exception {
        IgniteTx tx = inTx ? transactions().txStart() : null;

        IgniteCache<String, Integer> cache = jcache();

        cache.put("key1", 1);
        cache.put("key2", 2);

        // Check inside transaction.
        assert cache.get("key1") == 1;
        assert cache.get("key2") == 2;

        if (tx != null)
            tx.commit();

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
    public void testPutAsync() throws Exception {
        IgniteTx tx = txEnabled() ? transactions().txStart() : null;

        IgniteCache<String, Integer> cacheAsync = jcache().withAsync();

        jcache().put("key2", 1);

        cacheAsync.put("key1", 10);

        IgniteFuture<?> fut1 = cacheAsync.future();

        cacheAsync.put("key2", 11);

        IgniteFuture<?> fut2 = cacheAsync.future();

        IgniteFuture<IgniteTx> f = null;

        if (tx != null) {
            tx = (IgniteTx)tx.withAsync();

            tx.commit();

            f = tx.future();
        }

        fut1.get();
        fut2.get();

        assert f == null || f.get().state() == COMMITTED;

        checkSize(F.asSet("key1", "key2"));

        assert jcache().get("key1") == 10;
        assert jcache().get("key2") == 11;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testPutAll() throws Exception {
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
    public void testNullInTx() throws Exception {
        if (!txEnabled())
            return;

        final IgniteCache<String, Integer> cache = jcache();

        for (int i = 0; i < 100; i++) {
            final String key = "key-" + i;

            assertNull(cache.get(key));

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    IgniteTransactions txs = transactions();

                    try (IgniteTx tx = txs.txStart()) {
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

                    try (IgniteTx tx = txs.txStart()) {
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

                    try (IgniteTx tx = txs.txStart()) {
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
    public void testPutAllWithNulls() throws Exception {
        final IgniteCache<String, Integer> cache = jcache();

        {
            final Map<String, Integer> m = new LinkedHashMap<>(2);

            m.put("key1", 1);
            m.put(null, 2);

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override
                public Void call() throws Exception {
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
                @Override
                public Void call() throws Exception {
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
    public void testPutAllAsync() throws Exception {
        Map<String, Integer> map = F.asMap("key1", 1, "key2", 2);

        IgniteCache<String, Integer> cache = jcache();

        IgniteCache<String, Integer> cacheAsync = cache.withAsync();

        cacheAsync.putAll(map);

        IgniteFuture<?> f1 = cacheAsync.future();

        map.put("key1", 10);
        map.put("key2", 20);

        cacheAsync.putAll(map);

        IgniteFuture<?> f2 = cacheAsync.future();

        f2.get();
        f1.get();

        checkSize(F.asSet("key1", "key2"));

        assert cache.get("key1") == 10;
        assert cache.get("key2") == 20;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testGetAndPutIfAbsent() throws Exception {
        IgniteTx tx = txEnabled() ? transactions().txStart() : null;

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
                grid(i).jcache(null).localPeek("key", CachePeekMode.ONHEAP) + ']');
        }

        assertEquals((Integer)1, cache.getAndPutIfAbsent("key", 2));

        assert cache.get("key") != null;
        assert cache.get("key") == 1;

        // Check swap.
        cache.put("key2", 1);

        cache.localEvict(Collections.singleton("key2"));

        assertEquals((Integer)1, cache.getAndPutIfAbsent("key2", 3));

        // Check db.
        putToStore("key3", 3);

        assertEquals((Integer)3, cache.getAndPutIfAbsent("key3", 4));

        assertEquals((Integer)1, cache.get("key2"));
        assertEquals((Integer)3, cache.get("key3"));

        cache.localEvict(Collections.singleton("key2"));

        // Same checks inside tx.
        tx = txEnabled() ? transactions().txStart() : null;

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
    public void testGetAndPutIfAbsentAsync() throws Exception {
        IgniteTx tx = txEnabled() ? transactions().txStart() : null;

        IgniteCache<String, Integer> cache = jcache();

        IgniteCache<String, Integer> cacheAsync = cache.withAsync();

        try {
            cacheAsync.getAndPutIfAbsent("key", 1);

            IgniteFuture<Integer> fut1 = cacheAsync.future();

            assert fut1.get() == null;
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

        // Check swap.
        cache.put("key2", 1);

        cache.localEvict(Collections.singleton("key2"));

        cacheAsync.getAndPutIfAbsent("key2", 3);

        assertEquals((Integer)1, cacheAsync.<Integer>future().get());

        // Check db.
        putToStore("key3", 3);

        cacheAsync.getAndPutIfAbsent("key3", 4);

        assertEquals((Integer)3, cacheAsync.<Integer>future().get());

        cache.localEvict(Collections.singleton("key2"));

        // Same checks inside tx.
        tx = txEnabled() ? transactions().txStart() : null;

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
    public void testPutIfAbsent() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        assertNull(cache.get("key"));
        assert cache.putIfAbsent("key", 1);
        assert cache.get("key") != null && cache.get("key") == 1;
        assert !cache.putIfAbsent("key", 2);
        assert cache.get("key") != null && cache.get("key") == 1;

        // Check swap.
        cache.put("key2", 1);

        cache.localEvict(Collections.singleton("key2"));

        assertFalse(cache.putIfAbsent("key2", 3));

        // Check db.
        putToStore("key3", 3);

        assertFalse(cache.putIfAbsent("key3", 4));

        cache.localEvict(Collections.singleton("key2"));

        // Same checks inside tx.
        IgniteTx tx = txEnabled() ? transactions().txStart() : null;

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
    public void testPutxIfAbsentAsync() throws Exception {
        if (txEnabled())
            checkPutxIfAbsentAsync(true);
    }

    /**
     * @throws Exception In case of error.
     */
    public void testPutxIfAbsentAsyncNoTx() throws Exception {
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

        // Check swap.
        cache.put("key2", 1);

        cache.localEvict(Collections.singleton("key2"));

        cacheAsync.putIfAbsent("key2", 3);

        assertFalse(cacheAsync.<Boolean>future().get());

        // Check db.
        putToStore("key3", 3);

        cacheAsync.putIfAbsent("key3", 4);

        assertFalse(cacheAsync.<Boolean>future().get());

        cache.localEvict(Arrays.asList("key2"));

        // Same checks inside tx.
        IgniteTx tx = inTx ? transactions().txStart() : null;

        try {
            cacheAsync.putIfAbsent("key2", 3);

            assertFalse(cacheAsync.<Boolean>future().get());

            cacheAsync.putIfAbsent("key3", 4);

            assertFalse(cacheAsync.<Boolean>future().get());

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        assertEquals((Integer)1, cache.get("key2"));
        assertEquals((Integer)3, cache.get("key3"));
    }

    /**
     * @throws Exception In case of error.
     */
    public void testPutIfAbsentAsyncConcurrent() throws Exception {
        IgniteCache<String, Integer> cacheAsync = jcache().withAsync();

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
    public void testGetAndReplace() throws Exception {
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

        info("evict key");

        cache.localEvict(Collections.singleton("key"));

        info("key 3 -> 4");

        assert cache.replace("key", 3, 4);

        assert cache.get("key") == 4;

        putToStore("key2", 5);

        info("key2 5 -> 6");

        assert cache.replace("key2", 5, 6);

        for (int i = 0; i < gridCount(); i++) {
            info("Peek key on grid [i=" + i + ", nodeId=" + grid(i).localNode().id() +
                ", peekVal=" + grid(i).jcache(null).localPeek("key", CachePeekMode.ONHEAP) + ']');

            info("Peek key2 on grid [i=" + i + ", nodeId=" + grid(i).localNode().id() +
                ", peekVal=" + grid(i).jcache(null).localPeek("key2", CachePeekMode.ONHEAP) + ']');
        }

        assertEquals((Integer)6, cache.get("key2"));

        cache.localEvict(Collections.singleton("key"));

        IgniteTx tx = txEnabled() ? transactions().txStart() : null;

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
    public void testReplace() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key", 1);

        assert cache.get("key") == 1;

        assert cache.replace("key", 2);

        assert cache.get("key") == 2;

        assert !cache.replace("wrong", 2);

        cache.localEvict(Collections.singleton("key"));

        assert cache.replace("key", 4);

        assert cache.get("key") == 4;

        putToStore("key2", 5);

        assert cache.replace("key2", 6);

        assertEquals((Integer)6, cache.get("key2"));

        cache.localEvict(Collections.singleton("key"));

        IgniteTx tx = txEnabled() ? transactions().txStart() : null;

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
    public void testGetAndReplaceAsync() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        IgniteCache<String, Integer> cacheAsync = cache.withAsync();

        cache.put("key", 1);

        assert cache.get("key") == 1;

        cacheAsync.getAndReplace("key", 2);

        assert cacheAsync.future().get() == 1;

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

        cache.localEvict(Collections.singleton("key"));

        cacheAsync.replace("key", 3, 4);

        assert cacheAsync.<Boolean>future().get();

        assert cache.get("key") == 4;

        putToStore("key2", 5);

        cacheAsync.replace("key2", 5, 6);

        assert cacheAsync.<Boolean>future().get();

        assertEquals((Integer)6, cache.get("key2"));

        cache.localEvict(Collections.singleton("key"));

        IgniteTx tx = txEnabled() ? transactions().txStart() : null;

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
    public void testReplacexAsync() throws Exception {
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

        cache.localEvict(Collections.singleton("key"));

        cacheAsync.replace("key", 4);

        assert cacheAsync.<Boolean>future().get();

        assert cache.get("key") == 4;

        putToStore("key2", 5);

        cacheAsync.replace("key2", 6);

        assert cacheAsync.<Boolean>future().get();

        assert cache.get("key2") == 6;

        cache.localEvict(Collections.singleton("key"));

        IgniteTx tx = txEnabled() ? transactions().txStart() : null;

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
    public void testGetAndRemove() throws Exception {
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
    public void testDeletedEntriesFlag() throws Exception {
        if (cacheMode() != LOCAL && cacheMode() != REPLICATED) {
            int cnt = 3;

            IgniteCache<String, Integer> cache = jcache();

            for (int i = 0; i < cnt; i++)
                cache.put(String.valueOf(i), i);

            for (int i = 0; i < cnt; i++)
                cache.remove(String.valueOf(i));

            for (int g = 0; g < gridCount(); g++) {
                for (int i = 0; i < cnt; i++) {
                    String key = String.valueOf(i);

                    GridCacheContext<String, Integer> cctx = context(g);

                    GridCacheEntryEx<String, Integer> entry = cctx.isNear() ? cctx.near().dht().peekEx(key) :
                        cctx.cache().peekEx(key);

                    if (grid(0).affinity(null).mapKeyToPrimaryAndBackups(key).contains(grid(g).localNode())) {
                        assertNotNull(entry);
                        assertTrue(entry.deleted());
                    }
                    else
                        assertNull(entry);
                }
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveLoad() throws Exception {
        int cnt = 10;

        Set<String> keys = new HashSet<>();

        for (int i = 0; i < cnt; i++)
            keys.add(String.valueOf(i));

        jcache().removeAll(keys);

        for (String key : keys)
            putToStore(key, Integer.parseInt(key));

        for (int g = 0; g < gridCount(); g++)
            grid(g).jcache(null).localLoadCache(null);

        for (int g = 0; g < gridCount(); g++) {
            for (int i = 0; i < cnt; i++) {
                String key = String.valueOf(i);

                if (grid(0).affinity(null).mapKeyToPrimaryAndBackups(key).contains(grid(g).localNode()))
                    assertEquals((Integer)i, jcache(g).localPeek(key, CachePeekMode.ONHEAP));
                else
                    assertNull(jcache(g).localPeek(key, CachePeekMode.ONHEAP));
            }
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testRemoveAsync() throws Exception {
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

        assert cacheAsync.future().get() == 2;

        assert cache.get("key2") == null;

        cacheAsync.getAndRemove("key2");

        assert cacheAsync.future().get() == null;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testRemove() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key1", 1);

        assert cache.remove("key1");
        assert cache.get("key1") == null;
        assert !cache.remove("key1");
    }

    /**
     * @throws Exception In case of error.
     */
    public void testRemovexAsync() throws Exception {
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
    public void testGlobalRemoveAll() throws Exception {
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
            IgniteCache<String, Integer> asyncCache0 = jcache(gridCount() > 1 ? 1 : 0).withAsync();

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
    public void testRemoveAllWithNulls() throws Exception {
        final IgniteCache<String, Integer> cache = jcache();

        final Set<String> c = new LinkedHashSet<>();

        c.add("key1");
        c.add(null);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                cache.removeAll(c);

                return null;
            }
        }, NullPointerException.class, null);

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
    public void testRemoveAllDuplicates() throws Exception {
        jcache().removeAll(ImmutableSet.of("key1", "key1", "key1"));
    }

    /**
     * @throws Exception In case of error.
     */
    public void testRemoveAllDuplicatesTx() throws Exception {
        if (txEnabled()) {
            try (IgniteTx tx = transactions().txStart()) {
                jcache().removeAll(ImmutableSet.of("key1", "key1", "key1"));

                tx.commit();
            }
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testRemoveAllEmpty() throws Exception {
        jcache().removeAll();
    }

    /**
     * @throws Exception In case of error.
     */
    public void testRemoveAllAsync() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        IgniteCache<String, Integer> cacheAsync = cache.withAsync();

        cache.put("key1", 1);
        cache.put("key2", 2);
        cache.put("key3", 3);

        checkSize(F.asSet("key1", "key2", "key3"));

        cacheAsync.removeAll(F.asSet("key1", "key2"));

        cacheAsync.future().get();

        checkSize(F.asSet("key3"));

        checkContainsKey(false, "key1");
        checkContainsKey(false, "key2");
        checkContainsKey(true, "key3");
    }

    /**
     * @throws Exception In case of error.
     */
    public void testLoadAll() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        Set<String> keys = new HashSet<>(primaryKeysForCache(cache, 2));

        for (String key : keys)
            assertNull(cache.localPeek(key, CachePeekMode.ONHEAP));

        Map<String, Integer> vals = new HashMap<>();

        int i = 0;

        for (String key : keys) {
            cache.put(key, i);

            vals.put(key, i);

            i++;
        }

        for (String key : keys)
            assertEquals(vals.get(key), cache.localPeek(key, CachePeekMode.ONHEAP));

        cache.clear();

        for (String key : keys)
            assertNull(cache.localPeek(key, CachePeekMode.ONHEAP));

        loadAll(cache, keys, true);

        for (String key : keys)
            assertEquals(vals.get(key), cache.localPeek(key, CachePeekMode.ONHEAP));
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveAfterClear() throws Exception {
        IgniteEx ignite = grid(0);

        CacheDistributionMode distroMode = ignite.jcache(null).getConfiguration(CacheConfiguration.class).getDistributionMode();

        if (distroMode == CacheDistributionMode.NEAR_ONLY || distroMode == CacheDistributionMode.CLIENT_ONLY) {
            if (gridCount() < 2)
                return;

            ignite = grid(1);
        }

        IgniteCache<Integer, Integer> cache = ignite.jcache(null);

        int key = 0;

        Collection<Integer> keys = new ArrayList<>();

        for (int k = 0; k < 2; k++) {
            while (!ignite.affinity(null).isPrimary(ignite.localNode(), key))
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

            grid0.jcache(null).removeAll();

            assertTrue(grid0.jcache(null).localSize() == 0);
        }
    }

    /**
     *
     */
    private void xxx() {
        System.out.printf("");
    }

    /**
     * @throws Exception In case of error.
     */
    public void testClear() throws Exception {
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

                assertEquals(vals.get(first), peek(cache, first));
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

        assert cache.localSize(CachePeekMode.ONHEAP) == 0;

        cache.clear();

        cache.localPromote(ImmutableSet.of("key2", "key1"));

        assert cache.localPeek("key1", CachePeekMode.ONHEAP) == null;
        assert cache.localPeek("key2", CachePeekMode.ONHEAP) == null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testGlobalClearAll() throws Exception {
        globalClearAll(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGlobalClearAllAsync() throws Exception {
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
            IgniteCache<String, Integer> asyncCache = jcache().withAsync();

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
    public void testLockUnlock() throws Exception {
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
    public void testLockUnlockAll() throws Exception {
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
    public void testPeek() throws Exception {
        Ignite ignite = primaryIgnite("key");
        IgniteCache<String, Integer> cache = ignite.jcache(null);

        assert cache.localPeek("key", CachePeekMode.ONHEAP) == null;

        cache.put("key", 1);

        cache.replace("key", 2);

        assert cache.localPeek("key", CachePeekMode.ONHEAP) == 2;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPeekTxRemoveOptimistic() throws Exception {
        checkPeekTxRemove(OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPeekTxRemovePessimistic() throws Exception {
        checkPeekTxRemove(PESSIMISTIC);
    }

    /**
     * @param concurrency Concurrency.
     * @throws Exception If failed.
     */
    private void checkPeekTxRemove(IgniteTxConcurrency concurrency) throws Exception {
        if (txEnabled()) {
            Ignite ignite = primaryIgnite("key");
            IgniteCache<String, Integer> cache = ignite.jcache(null);

            cache.put("key", 1);

            try (IgniteTx tx = ignite.transactions().txStart(concurrency, READ_COMMITTED)) {
                cache.remove("key");

                assertNull(cache.get("key")); // localPeek ignores transactions.
                assertNotNull(cache.localPeek("key")); // localPeek ignores transactions.

                tx.commit();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPeekRemove() throws Exception {
        IgniteCache<String, Integer> cache = primaryCache("key");

        cache.put("key", 1);
        cache.remove("key");

        assertNull(cache.localPeek("key", CachePeekMode.ONHEAP));
    }

    /**
     * @throws Exception In case of error.
     */
    public void testPeekMode() throws Exception {
        String key = "testPeekMode";

        GridCache<String, Integer> cache = ((IgniteKernal)primaryIgnite(key)).cache(null);

        cache.put(key, 1);

        assert cache.peek(key, F.asList(TX)) == null;
        assert cache.peek(key, F.asList(SWAP)) == null;
        assert cache.peek(key, F.asList(DB)) == 1;
        assert cache.peek(key, F.asList(TX, GLOBAL)) == 1;

        if (cacheMode() == LOCAL) {
            assert cache.peek(key, F.asList(TX, NEAR_ONLY)) == 1;
            assert cache.peek(key, F.asList(TX, PARTITIONED_ONLY)) == 1;
        }

        assert cache.peek(key, F.asList(SMART)) == 1;

        assert cache.peek("wrongKey", F.asList(TX, GLOBAL, SWAP, DB)) == null;

        if (cacheMode() == LOCAL) {
            assert cache.peek("wrongKey", F.asList(TX, NEAR_ONLY, SWAP, DB)) == null;
            assert cache.peek("wrongKey", F.asList(TX, PARTITIONED_ONLY, SWAP, DB)) == null;
        }

        if (txEnabled()) {
            IgniteTx tx = cache.txStart();

            cache.replace(key, 2);

            assert cache.peek(key, F.asList(GLOBAL)) == 1;

            if (cacheMode() == LOCAL) {
                assert cache.peek(key, F.asList(NEAR_ONLY)) == 1;
                assert cache.peek(key, F.asList(PARTITIONED_ONLY)) == 1;
            }

            assert cache.peek(key, F.asList(TX)) == 2;
            assert cache.peek(key, F.asList(SMART)) == 2;
            assert cache.peek(key, F.asList(SWAP)) == null;
            assert cache.peek(key, F.asList(DB)) == 1;

            tx.commit();
        }
        else
            cache.replace(key, 2);

        assertEquals((Integer)2, cache.peek(key, F.asList(GLOBAL)));

        if (cacheMode() == LOCAL) {
            assertEquals((Integer)2, cache.peek(key, F.asList(NEAR_ONLY)));
            assertEquals((Integer)2, cache.peek(key, F.asList(PARTITIONED_ONLY)));
        }

        assertNull(cache.peek(key, F.asList(TX)));
        assertNull(cache.peek(key, F.asList(SWAP)));
        assertEquals((Integer)2, cache.peek(key, F.asList(DB)));

        assertTrue(cache.evict(key));

        assertNull(cache.peek(key, F.asList(SMART)));
        assertNull(cache.peek(key, F.asList(TX, GLOBAL)));

        if (cacheMode() == LOCAL) {
            assertNull(cache.peek(key, F.asList(TX, NEAR_ONLY)));
            assertNull(cache.peek(key, F.asList(TX, PARTITIONED_ONLY)));
        }

        assertEquals((Integer)2, cache.peek(key, F.asList(SWAP)));
        assertEquals((Integer)2, cache.peek(key, F.asList(DB)));
        assertEquals((Integer)2, cache.peek(key, F.asList(SMART, SWAP, DB)));

        assertEquals((Integer)2, cache.peek(key, F.asList(SWAP)));
    }

    /**
     * @throws Exception In case of error.
     */
    public void testEvictExpired() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        String key = primaryKeysForCache(cache, 1).get(0);

        cache.put(key, 1);

        assertEquals((Integer)1, cache.get(key));

        long ttl = 500;

        final ExpiryPolicy expiry = new TouchedExpiryPolicy(new Duration(MILLISECONDS, ttl));

        grid(0).jcache(null).withExpiryPolicy(expiry).put(key, 1);

        Thread.sleep(ttl + 100);

        // Expired entry should not be swapped.
        cache.localEvict(Collections.singleton(key));

        assertNull(cache.localPeek(key, CachePeekMode.ONHEAP));

        cache.localPromote(Collections.singleton(key));

        assertNull(cache.localPeek(key, CachePeekMode.ONHEAP));

        assertTrue(cache.localSize() == 0);

        load(cache, key, true);

        CacheAffinity<String> aff = ignite(0).affinity(null);

        for (int i = 0; i < gridCount(); i++) {
            if (aff.isPrimary(grid(i).cluster().localNode(), key))
                assertEquals((Integer)1, peek(jcache(i), key));

            if (aff.isBackup(grid(i).cluster().localNode(), key))
                assertEquals((Integer)1, peek(jcache(i), key));
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPeekExpired() throws Exception {
        IgniteCache<String, Integer> c = jcache();

        String key = primaryKeysForCache(c, 1).get(0);

        info("Using key: " + key);

        c.put(key, 1);

        assertEquals(Integer.valueOf(1), c.localPeek(key, CachePeekMode.ONHEAP));

        int ttl = 500;

        final ExpiryPolicy expiry = new TouchedExpiryPolicy(new Duration(MILLISECONDS, ttl));

        c.withExpiryPolicy(expiry).put(key, 1);

        Thread.sleep(ttl + 100);

        assert c.localPeek(key, CachePeekMode.ONHEAP) == null;

        assert c.localSize() == 0 : "Cache is not empty.";
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPeekExpiredTx() throws Exception {
        if (txEnabled()) {
            IgniteCache<String, Integer> c = jcache();

            String key = "1";
            int ttl = 500;

            try (IgniteTx tx = grid(0).ignite().transactions().txStart()) {
                final ExpiryPolicy expiry = new TouchedExpiryPolicy(new Duration(MILLISECONDS, ttl));

                grid(0).jcache(null).withExpiryPolicy(expiry).put(key, 1);

                tx.commit();
            }

            Thread.sleep(ttl + 100);

            assertNull(c.localPeek(key, CachePeekMode.ONHEAP));

            assert c.localSize() == 0;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTtlTx() throws Exception {
        if (txEnabled())
            checkTtl(true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTtlNoTx() throws Exception {
        checkTtl(false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTtlNoTxOldEntry() throws Exception {
        checkTtl(false, true);
    }

    /**
     * @param inTx In tx flag.
     * @param oldEntry {@code True} to check TTL on old entry, {@code false} on new.
     * @throws Exception If failed.
     */
    private void checkTtl(boolean inTx, boolean oldEntry) throws Exception {
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
            IgniteTx tx = transactions().txStart();

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
        IgniteTx tx = inTx ? transactions().txStart() : null;

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
            if (grid(i).affinity(null).isPrimaryOrBackup(grid(i).localNode(), key)) {
                GridCacheAdapter<String, Integer> cache = internalCache(jcache(i));

                if (cache.context().isNear())
                    cache = cache.context().near().dht();

                GridCacheEntryEx<String, Integer> curEntry = cache.peekEx(key);

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
            if (grid(i).affinity(null).isPrimaryOrBackup(grid(i).localNode(), key)) {
                GridCacheAdapter<String, Integer> cache = internalCache(jcache(i));

                if (cache.context().isNear())
                    cache = cache.context().near().dht();

                GridCacheEntryEx<String, Integer> curEntry = cache.peekEx(key);

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
            if (grid(i).affinity(null).isPrimaryOrBackup(grid(i).localNode(), key)) {
                GridCacheAdapter<String, Integer> cache = internalCache(jcache(i));

                if (cache.context().isNear())
                    cache = cache.context().near().dht();

                GridCacheEntryEx<String, Integer> curEntry = cache.peekEx(key);

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
            if (grid(i).affinity(null).isPrimaryOrBackup(grid(i).localNode(), key)) {
                GridCacheAdapter<String, Integer> cache = internalCache(jcache(i));

                if (cache.context().isNear())
                    cache = cache.context().near().dht();

                GridCacheEntryEx<String, Integer> curEntry = cache.peekEx(key);

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
    public void testLocalEvict() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        List<String> keys = primaryKeysForCache(cache, 3);

        String key1 = keys.get(0);
        String key2 = keys.get(1);
        String key3 = keys.get(2);

        cache.put(key1, 1);
        cache.put(key2, 2);
        cache.put(key3, 3);

        assert cache.localPeek(key1, CachePeekMode.ONHEAP) == 1;
        assert cache.localPeek(key2, CachePeekMode.ONHEAP) == 2;
        assert cache.localPeek(key3, CachePeekMode.ONHEAP) == 3;

        cache.localEvict(F.asList(key1, key2));

        assert cache.localPeek(key1, CachePeekMode.ONHEAP) == null;
        assert cache.localPeek(key2, CachePeekMode.ONHEAP) == null;
        assert cache.localPeek(key3, CachePeekMode.ONHEAP) == 3;

        loadAll(cache, ImmutableSet.of(key1, key2), true);

        CacheAffinity<String> aff = ignite(0).affinity(null);

        for (int i = 0; i < gridCount(); i++) {
            if (aff.isPrimaryOrBackup(grid(i).cluster().localNode(), key1))
                assertEquals((Integer)1, peek(jcache(i), key1));

            if (aff.isPrimaryOrBackup(grid(i).cluster().localNode(), key2))
                assertEquals((Integer)2, peek(jcache(i), key2));

            if (aff.isPrimaryOrBackup(grid(i).cluster().localNode(), key3))
                assertEquals((Integer)3, peek(jcache(i), key3));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testUnswap() throws Exception {
        GridCache<String, Integer> cache = cache();

        List<String> keys = primaryKeysForCache(jcache(), 3);

        String k1 = keys.get(0);
        String k2 = keys.get(1);
        String k3 = keys.get(2);

        cache.put(k1, 1);
        cache.put(k2, 2);
        cache.put(k3, 3);

        final AtomicInteger swapEvts = new AtomicInteger(0);
        final AtomicInteger unswapEvts = new AtomicInteger(0);

        Collection<String> locKeys = new HashSet<>();

        if (CU.isAffinityNode(cache.configuration())) {
            locKeys.addAll(cache.primaryKeySet());

            info("Local keys (primary): " + locKeys);

            locKeys.addAll(cache.keySet(new IgnitePredicate<Cache.Entry<String, Integer>>() {
                @Override public boolean apply(Cache.Entry<String, Integer> e) {
                    return grid(0).affinity(null).isBackup(grid(0).localNode(), e.getKey());
                }
            }));

            info("Local keys (primary + backup): " + locKeys);
        }

        for (int i = 0; i < gridCount(); i++) {
            grid(i).events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    info("Received event: " + evt);

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
            }, EVT_CACHE_OBJECT_SWAPPED, EVT_CACHE_OBJECT_UNSWAPPED);
        }

        assert cache.evict(k2);
        assert cache.evict(k3);

        assert cache.containsKey(k1);
        assert !cache.containsKey(k2);
        assert !cache.containsKey(k3);

        int cnt = 0;

        if (locKeys.contains(k2)) {
            assertEquals((Integer)2, cache.promote(k2));

            cnt++;
        }
        else
            assertNull(cache.promote(k2));

        if (locKeys.contains(k3)) {
            assertEquals((Integer)3, cache.promote(k3));

            cnt++;
        }
        else
            assertNull(cache.promote(k3));

        assertEquals(cnt, swapEvts.get());
        assertEquals(cnt, unswapEvts.get());

        assert cache.evict(k1);

        assertEquals((Integer)1, cache.get(k1));

        if (locKeys.contains(k1))
            cnt++;

        assertEquals(cnt, swapEvts.get());
        assertEquals(cnt, unswapEvts.get());

        cache.clear();

        // Check with multiple arguments.
        cache.put(k1, 1);
        cache.put(k2, 2);
        cache.put(k3, 3);

        swapEvts.set(0);
        unswapEvts.set(0);

        cache.evict(k2);
        cache.evict(k3);

        assert cache.containsKey(k1);
        assert !cache.containsKey(k2);
        assert !cache.containsKey(k3);

        cache.promoteAll(F.asList(k2, k3));

        cnt = 0;

        if (locKeys.contains(k2))
            cnt++;

        if (locKeys.contains(k3))
            cnt++;

        assertEquals(cnt, swapEvts.get());
        assertEquals(cnt, unswapEvts.get());
    }

    /**
     * JUnit.
     */
    public void testCacheProxy() {
        IgniteCache<String, Integer> cache = jcache();

        assert cache instanceof IgniteCacheProxy;
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testCompactExpired() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        String key = F.first(primaryKeysForCache(cache, 1));

        cache.put(key, 1);

        long ttl = 500;

        final ExpiryPolicy expiry = new TouchedExpiryPolicy(new Duration(MILLISECONDS, ttl));

        grid(0).jcache(null).withExpiryPolicy(expiry).put(key, 1);

        Thread.sleep(ttl + 100);

        // Peek will actually remove entry from cache.
        assertNull(cache.localPeek(key, CachePeekMode.ONHEAP));

        assert cache.localSize() == 0;
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testOptimisticTxMissingKey() throws Exception {
        if (txEnabled()) {
            try (IgniteTx tx = transactions().txStart(OPTIMISTIC, READ_COMMITTED)) {
                // Remove missing key.
                assertTrue(jcache().remove(UUID.randomUUID().toString()));

                tx.commit();
            }
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testOptimisticTxMissingKeyNoCommit() throws Exception {
        if (txEnabled()) {
            try (IgniteTx tx = transactions().txStart(OPTIMISTIC, READ_COMMITTED)) {
                // Remove missing key.
                assertTrue(jcache().remove(UUID.randomUUID().toString()));

                tx.setRollbackOnly();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticTxReadCommittedInTx() throws Exception {
        checkRemovexInTx(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticTxRepeatableReadInTx() throws Exception {
        checkRemovexInTx(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticTxReadCommittedInTx() throws Exception {
        checkRemovexInTx(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticTxRepeatableReadInTx() throws Exception {
        checkRemovexInTx(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws Exception If failed.
     */
    private void checkRemovexInTx(IgniteTxConcurrency concurrency, IgniteTxIsolation isolation) throws Exception {
        if (txEnabled()) {
            final int cnt = 10;

            CU.inTx(ignite(0), jcache(), concurrency, isolation, new CIX1<IgniteCache<String, Integer>>() {
                @Override public void applyx(IgniteCache<String, Integer> cache) {
                    for (int i = 0; i < cnt; i++)
                        cache.put("key" + i, i);
                }
            });

            CU.inTx(ignite(0), jcache(), concurrency, isolation, new CIX1<IgniteCache<String, Integer>>() {
                @Override public void applyx(IgniteCache<String, Integer> cache) {
                    for (int i = 0; i < cnt; i++)
                        assertEquals(new Integer(i), cache.get("key" + i));
                }
            });

            CU.inTx(ignite(0), jcache(), concurrency, isolation, new CIX1<IgniteCache<String, Integer>>() {
                @Override public void applyx(IgniteCache<String, Integer> cache) throws IgniteCheckedException {
                    for (int i = 0; i < cnt; i++)
                        assertTrue(cache.remove("key" + i));
                }
            });
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPessimisticTxMissingKey() throws Exception {
        if (txEnabled()) {
            try (IgniteTx tx = transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
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
    public void testPessimisticTxMissingKeyNoCommit() throws Exception {
        if (txEnabled()) {
            try (IgniteTx tx = transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                // Remove missing key.
                assertFalse(jcache().remove(UUID.randomUUID().toString()));

                tx.setRollbackOnly();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticTxRepeatableRead() throws Exception {
        if (txEnabled()) {
            try (IgniteTx ignored = transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                jcache().put("key", 1);

                assert jcache().get("key") == 1;
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticTxRepeatableReadOnUpdate() throws Exception {
        if (txEnabled()) {
            try (IgniteTx ignored = transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                jcache().put("key", 1);

                assert jcache().getAndPut("key", 2) == 1;
            }
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testToMap() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        if (offheapTiered(cache))
            return;

        cache.put("key1", 1);
        cache.put("key2", 2);

        Map<String, Integer> map = new HashMap<>();

        for (int i = 0; i < gridCount(); i++) {
            for (Cache.Entry<String, Integer> entry : jcache(i))
                map.put(entry.getKey(), entry.getValue());
        }

        assert map != null;
        assert map.size() == 2;
        assert map.get("key1") == 1;
        assert map.get("key2") == 2;
    }

    /**
     * @param keys Expected keys.
     * @throws Exception If failed.
     */
    protected void checkSize(Collection<String> keys) throws Exception {
        if (nearEnabled())
            assertEquals(keys.size(), jcache().localSize());
        else {
            for (int i = 0; i < gridCount(); i++) {
                GridCacheContext<String, Integer> ctx = context(i);

                if (offheapTiered(ctx.cache()))
                    continue;

                int size = 0;

                for (String key : keys) {
                    if (ctx.affinity().localNode(key, ctx.discovery().topologyVersion())) {
                        GridCacheEntryEx<String, Integer> e =
                            ctx.isNear() ? ctx.near().dht().peekEx(key) : ctx.cache().peekEx(key);

                        assert e != null : "Entry is null [idx=" + i + ", key=" + key + ", ctx=" + ctx + ']';
                        assert !e.deleted() : "Entry is deleted: " + e;

                        size++;
                    }
                }

                assertEquals("Incorrect size on cache #" + i, size, jcache(i).localSize());
            }
        }
    }

    /**
     * @param keys Expected keys.
     * @throws Exception If failed.
     */
    protected void checkKeySize(Collection<String> keys) throws Exception {
        if (nearEnabled())
            assertEquals("Invalid key size: " + jcache().localSize(), keys.size(), jcache().localSize());
        else {
            for (int i = 0; i < gridCount(); i++) {
                GridCacheContext<String, Integer> ctx = context(i);

                int size = 0;

                for (String key : keys)
                    if (ctx.affinity().localNode(key, ctx.discovery().topologyVersion()))
                        size++;

                assertEquals("Incorrect key size on cache #" + i, size, jcache(i).localSize());
            }
        }
    }

    /**
     * @param exp Expected value.
     * @param key Key.
     * @throws Exception If failed.
     */
    private void checkContainsKey(boolean exp, String key) throws Exception {
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
        ClusterNode node = grid(0).affinity(null).mapKeyToNode(key);

        if (node == null)
            throw new IgniteException("Failed to find primary node.");

        UUID nodeId = node.id();

        for (int i = 0; i < gridCount(); i++) {
            if (context(i).localNodeId().equals(nodeId))
                return ignite(i);
        }

        throw new IgniteException("Failed to find primary node.");
    }

    /**
     * @param key Key.
     * @return Cache.
     */
    protected IgniteCache<String, Integer> primaryCache(String key) {
        return primaryIgnite(key).jcache(null);
    }

    /**
     * @param cache Cache.
     * @param cnt Keys count.
     * @return Collection of keys for which given cache is primary.
     */
    protected List<String> primaryKeysForCache(IgniteCache<String, Integer> cache, int cnt, int startFrom) {
        List<String> found = new ArrayList<>(cnt);

        Ignite ignite = cache.unwrap(Ignite.class);
        CacheAffinity<Object> affinity = ignite.affinity(cache.getName());

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

    /**
     * @param cache Cache.
     * @param cnt Keys count.
     * @return Collection of keys for which given cache is primary.
     * @throws IgniteCheckedException If failed.
     */
    protected List<String> primaryKeysForCache(IgniteCache<String, Integer> cache, int cnt)
        throws IgniteCheckedException {
        return primaryKeysForCache(cache, cnt, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIgniteCacheIterator() throws Exception {
        IgniteCache<String, Integer> cache = jcache(0);

        assertFalse(cache.iterator().hasNext());

        Map<String, Integer> entries = new HashMap<>();

        for (int i = 0; i < 20000; ++i) {
            cache.put(Integer.toString(i), i);

            entries.put(Integer.toString(i), i);

            if (i > 0 && i % 500 == 0)
                info("Puts finished: " + i);
        }

        checkIteratorHasNext();

        checkIteratorCache(entries);

        checkIteratorRemove(cache, entries);

        checkIteratorEmpty(cache);
    }

    /**
     * If hasNext() is called repeatedly, it should return the same result.
     */
    private void checkIteratorHasNext() {
        Iterator<Cache.Entry<String, Integer>> iter = jcache(0).iterator();

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
        final Iterator<Cache.Entry<String, Integer>> iter = jcache(0).iterator();

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
    private void checkIteratorCache(IgniteCache<String, Integer> cache, Map<String, Integer> entries) {
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
        for (int j = 0; j < gridCount(); j++) {

            GridCacheQueryManager queries = context(j).queries();

            Map map = GridTestUtils.getFieldValue(queries, GridCacheQueryManager.class, "qryIters");

            for (Object obj : map.values())
                assertEquals("Iterators not removed for grid " + j, 0, ((Map) obj).size());
        }
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

                Thread.sleep(500);
            }
        }
    }

    /**
     * @param cache Cache.
     * @throws Exception If failed.
     */
    protected void atomicClockModeDelay(IgniteCache cache) throws Exception {
        CacheConfiguration ccfg = (CacheConfiguration)cache.getConfiguration(CacheConfiguration.class);

        if (ccfg.getCacheMode() != LOCAL &&
            ccfg.getAtomicityMode() == CacheAtomicityMode.ATOMIC &&
            ccfg.getAtomicWriteOrderMode() == CacheAtomicWriteOrderMode.CLOCK)
            U.sleep(100);
    }
}
