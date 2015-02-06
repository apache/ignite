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
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.transactions.*;

import javax.cache.expiry.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Cache metrics test.
 */
public abstract class GridCacheAbstractMetricsSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int KEY_CNT = 50;

    /** {@inheritDoc} */
    @Override protected boolean swapEnabled() {
        return false;
    }

    /**
     * @return Key count.
     */
    protected int keyCount() {
        return KEY_CNT;
    }

    /**
     * Gets number of inner reads per "put" operation.
     *
     * @param isPrimary {@code true} if local node is primary for current key, {@code false} otherwise.
     * @return Expected number of inner reads.
     */
    protected int expectedReadsPerPut(boolean isPrimary) {
        return isPrimary ? 1 : 2;
    }

    /**
     * Gets number of missed per "put" operation.
     *
     * @param isPrimary {@code true} if local node is primary for current key, {@code false} otherwise.
     * @return Expected number of misses.
     */
    protected int expectedMissesPerPut(boolean isPrimary) {
        return isPrimary ? 1 : 2;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        for (int i = 0; i < gridCount(); i++) {
            Ignite g = grid(i);

            g.cache(null).removeAll();

            assert g.cache(null).isEmpty();

            g.cache(null).mxBean().clear();

            g.transactions().resetMetrics();
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        for (int i = 0; i < gridCount(); i++) {
            Ignite g = grid(i);

            g.cache(null).configuration().setStatisticsEnabled(true);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetMetricsSnapshot() throws Exception {
        IgniteCache<Object, Object> cache = grid(0).jcache(null);

        assertNotSame("Method metrics() should return snapshot.", cache.metrics(), cache.metrics());
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveAsyncAvgTime() throws Exception {
        GridCache<Object, Object> cache = grid(0).cache(null);

        cache.putx(1, 1);
        cache.putx(2, 2);

        assertEquals(cache.metrics().getAverageRemoveTime(), 0.0, 0.0);

        IgniteInternalFuture<Object> fut = cache.removeAsync(1);

        assertEquals(1, (int)fut.get());

        assert cache.metrics().getAverageRemoveTime() > 0;

        fut = cache.removeAsync(2);

        assertEquals(2, (int)fut.get());

        assert cache.metrics().getAverageRemoveTime() > 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveAsyncValAvgTime() throws Exception {
        GridCache<Object, Object> cache = grid(0).cache(null);

        Integer key = 0;

        for (int i = 0; i < 1000; i++) {
            if (cache.affinity().isPrimary(grid(0).localNode(), i)) {
                key = i;

                break;
            }
        }

        assertEquals(cache.metrics().getAverageRemoveTime(), 0.0, 0.0);

        cache.put(key, key);

        IgniteInternalFuture<Boolean> fut = cache.removeAsync(key, key);

        assertTrue(fut.get());

        assert cache.metrics().getAverageRemoveTime() >= 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveAvgTime() throws Exception {
        IgniteCache<Integer, Integer> jcache = grid(0).jcache(null);
        GridCache<Object, Object> cache = grid(0).cache(null);

        jcache.put(1, 1);
        jcache.put(2, 2);

        assertEquals(cache.metrics().getAverageRemoveTime(), 0.0, 0.0);

        jcache.remove(1);

        float avgRmvTime = cache.metrics().getAverageRemoveTime();

        assert avgRmvTime > 0;

        jcache.remove(2);

        assert cache.metrics().getAverageRemoveTime() > 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveAllAvgTime() throws Exception {
        IgniteCache<Integer, Integer> jcache = grid(0).jcache(null);
        GridCache<Object, Object> cache = grid(0).cache(null);

        jcache.put(1, 1);
        jcache.put(2, 2);
        jcache.put(3, 3);

        assertEquals(cache.metrics().getAverageRemoveTime(), 0.0, 0.0);

        Set<Integer> keys = new HashSet<>(4, 1);
        keys.add(1);
        keys.add(2);
        keys.add(3);

        jcache.removeAll(keys);

        float averageRemoveTime = cache.metrics().getAverageRemoveTime();

        assert averageRemoveTime >= 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveAllAsyncAvgTime() throws Exception {
        GridCache<Object, Object> cache = grid(0).cache(null);

        Set<Integer> keys = new LinkedHashSet<>();

        for (int i = 0; i < 1000; i++) {
            if (cache.affinity().isPrimary(grid(0).localNode(), i)) {
                keys.add(i);

                cache.put(i, i);

                if(keys.size() == 3)
                    break;
            }
        }

        assertEquals(cache.metrics().getAverageRemoveTime(), 0.0, 0.0);

        IgniteInternalFuture<?> fut = cache.removeAllAsync(keys);

        fut.get();

        assert cache.metrics().getAverageRemoveTime() >= 0;
    }


    /**
     * @throws Exception If failed.
     */
    public void testGetAvgTime() throws Exception {
        IgniteCache<Integer, Integer> jcache = grid(0).jcache(null);
        GridCache<Object, Object> cache = grid(0).cache(null);

        jcache.put(1, 1);

        assertEquals(0.0, cache.metrics().getAverageGetTime(), 0.0);

        jcache.get(1);

        float averageGetTime = cache.metrics().getAverageGetTime();

        assert averageGetTime > 0;

        jcache.get(2);

        assert cache.metrics().getAverageGetTime() > 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAllAvgTime() throws Exception {
        IgniteCache<Integer, Integer> jcache = grid(0).jcache(null);
        GridCache<Object, Object> cache = grid(0).cache(null);

        assertEquals(0.0, cache.metrics().getAverageGetTime(), 0.0);

        jcache.put(1, 1);
        jcache.put(2, 2);
        jcache.put(3, 3);

        assertEquals(0.0, cache.metrics().getAverageGetTime(), 0.0);

        Set<Integer> keys = new TreeSet<>();
        keys.add(1);
        keys.add(2);
        keys.add(3);

        jcache.getAll(keys);

        assert cache.metrics().getAverageGetTime() > 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAllAsyncAvgTime() throws Exception {
        GridCache<Object, Object> cache = grid(0).cache(null);

        assertEquals(0.0, cache.metrics().getAverageGetTime(), 0.0);

        cache.putx(1, 1);
        cache.putx(2, 2);
        cache.putx(3, 3);

        assertEquals(0.0, cache.metrics().getAverageGetTime(), 0.0);

        Set<Integer> keys = new TreeSet<>();
        keys.add(1);
        keys.add(2);
        keys.add(3);

        IgniteInternalFuture<Map<Object, Object>> fut = cache.getAllAsync(keys);

        fut.get();

        TimeUnit.MILLISECONDS.sleep(100L);

        assert cache.metrics().getAverageGetTime() > 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAvgTime() throws Exception {
        IgniteCache<Integer, Integer> jcache = grid(0).jcache(null);
        GridCache<Object, Object> cache = grid(0).cache(null);

        assertEquals(0.0, cache.metrics().getAveragePutTime(), 0.0);
        assertEquals(0, cache.metrics().getCachePuts());

        jcache.put(1, 1);

        float avgPutTime = cache.metrics().getAveragePutTime();

        assert avgPutTime >= 0;

        assertEquals(1, cache.metrics().getCachePuts());

        jcache.put(2, 2);

        assert cache.metrics().getAveragePutTime() >= 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutxAsyncAvgTime() throws Exception {
        GridCache<Object, Object> cache = grid(0).cache(null);

        assertEquals(0.0, cache.metrics().getAveragePutTime(), 0.0);
        assertEquals(0, cache.metrics().getCachePuts());

        IgniteInternalFuture<Boolean> fut = cache.putxAsync(1, 1);

        fut.get();

        TimeUnit.MILLISECONDS.sleep(100L);

        assert cache.metrics().getAveragePutTime() > 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAsyncAvgTime() throws Exception {
        GridCache<Object, Object> cache = grid(0).cache(null);

        Integer key = null;

        for (int i = 0; i < 1000; i++) {
            if (cache.affinity().isPrimary(grid(0).localNode(), i)) {
                key = i;

                break;
            }
        }

        assertEquals(0.0, cache.metrics().getAveragePutTime(), 0.0);
        assertEquals(0.0, cache.metrics().getAverageGetTime(), 0.0);

        IgniteInternalFuture<?> fut = cache.putAsync(key, key);

        fut.get();

        TimeUnit.MILLISECONDS.sleep(100L);

        assert cache.metrics().getAveragePutTime() > 0;
        assert cache.metrics().getAverageGetTime() > 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutxIfAbsentAsyncAvgTime() throws Exception {
        GridCache<Object, Object> cache = grid(0).cache(null);

        Integer key = null;

        for (int i = 0; i < 1000; i++) {
            if (cache.affinity().isPrimary(grid(0).localNode(), i)) {
                key = i;

                break;
            }
        }

        assertEquals(0.0f, cache.metrics().getAveragePutTime());

        IgniteInternalFuture<Boolean> fut = cache.putxIfAbsentAsync(key, key);

        fut.get();

        TimeUnit.MILLISECONDS.sleep(100L);

        assert cache.metrics().getAveragePutTime() > 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutIfAbsentAsyncAvgTime() throws Exception {
        GridCache<Object, Object> cache = grid(0).cache(null);

        Integer key = null;

        for (int i = 0; i < 1000; i++) {
            if (cache.affinity().isPrimary(grid(0).localNode(), i)) {
                key = i;

                break;
            }
        }

        assertEquals(0.0f, cache.metrics().getAveragePutTime());

        IgniteInternalFuture<?> fut = cache.putIfAbsentAsync(key, key);

        fut.get();

        TimeUnit.MILLISECONDS.sleep(100L);

        assert cache.metrics().getAveragePutTime() > 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllAvgTime() throws Exception {
        IgniteCache<Integer, Integer> jcache = grid(0).jcache(null);
        GridCache<Object, Object> cache = grid(0).cache(null);

        assertEquals(0.0, cache.metrics().getAveragePutTime(), 0.0);
        assertEquals(0, cache.metrics().getCachePuts());

        Map<Integer, Integer> values = new HashMap<>();

        values.put(1, 1);
        values.put(2, 2);
        values.put(3, 3);

        jcache.putAll(values);

        float averagePutTime = cache.metrics().getAveragePutTime();

        assert averagePutTime >= 0;
        assertEquals(values.size(), cache.metrics().getCachePuts());
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutsReads() throws Exception {
        GridCache<Integer, Integer> cache0 = grid(0).cache(null);

        int keyCnt = keyCount();

        int expReads = 0;
        int expMisses = 0;

        // Put and get a few keys.
        for (int i = 0; i < keyCnt; i++) {
            cache0.put(i, i); // +1 put

            boolean isPrimary = cache0.affinity().isPrimary(grid(0).localNode(), i);

            expReads += expectedReadsPerPut(isPrimary);
            expMisses += expectedMissesPerPut(isPrimary);

            info("Puts: " + cache0.metrics().getCachePuts());

            for (int j = 0; j < gridCount(); j++) {
                GridCache<Integer, Integer> cache = grid(j).cache(null);

                int cacheWrites = (int)cache.metrics().getCachePuts();

                assertEquals("Wrong cache metrics [i=" + i + ", grid=" + j + ']', i + 1, cacheWrites);
            }

            assertEquals("Wrong value for key: " + i, Integer.valueOf(i), cache0.get(i)); // +1 read

            expReads++;
        }

        // Check metrics for the whole cache.
        int puts = 0;
        int reads = 0;
        int hits = 0;
        int misses = 0;

        for (int i = 0; i < gridCount(); i++) {
            CacheMetrics m = grid(i).cache(null).metrics();

            puts += m.getCachePuts();
            reads += m.getCacheGets();
            hits += m.getCacheHits();
            misses += m.getCacheMisses();
        }

        info("Stats [reads=" + reads + ", hits=" + hits + ", misses=" + misses + ']');

        assertEquals(keyCnt * gridCount(), puts);
        assertEquals(expReads, reads);
        assertEquals(keyCnt, hits);
        assertEquals(expMisses, misses);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMissHitPercentage() throws Exception {
        GridCache<Integer, Integer> cache0 = grid(0).cache(null);

        int keyCnt = keyCount();

        // Put and get a few keys.
        for (int i = 0; i < keyCnt; i++) {
            cache0.put(i, i); // +1 read

            info("Puts: " + cache0.metrics().getCachePuts());

            for (int j = 0; j < gridCount(); j++) {
                GridCache<Integer, Integer> cache = grid(j).cache(null);

                long cacheWrites = cache.metrics().getCachePuts();

                assertEquals("Wrong cache metrics [i=" + i + ", grid=" + j + ']', i + 1, cacheWrites);
            }

            assertEquals("Wrong value for key: " + i, Integer.valueOf(i), cache0.get(i)); // +1 read
        }

        // Check metrics for the whole cache.
        for (int i = 0; i < gridCount(); i++) {
            CacheMetrics m = grid(i).cache(null).metrics();

            assertEquals(m.getCacheHits() * 100f / m.getCacheGets(), m.getCacheHitPercentage(), 0.1f);
            assertEquals(m.getCacheMisses() * 100f / m.getCacheGets(), m.getCacheMissPercentage(), 0.1f);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMisses() throws Exception {
        GridCache<Integer, Integer> cache = grid(0).cache(null);

        int keyCnt = keyCount();

        int expReads = 0;

        // Get a few keys missed keys.
        for (int i = 0; i < keyCnt; i++) {
            assertNull("Value is not null for key: " + i, cache.get(i));

            if (cache.configuration().getCacheMode() == CacheMode.REPLICATED ||
                cache.affinity().isPrimary(grid(0).localNode(), i))
                expReads++;
            else
                expReads += 2;
        }

        // Check metrics for the whole cache.
        long puts = 0;
        long reads = 0;
        long hits = 0;
        long misses = 0;

        for (int i = 0; i < gridCount(); i++) {
            CacheMetrics m = grid(i).cache(null).metrics();

            puts += m.getCachePuts();
            reads += m.getCacheGets();
            hits += m.getCacheHits();
            misses += m.getCacheMisses();
        }

        assertEquals(0, puts);
        assertEquals(expReads, reads);
        assertEquals(0, hits);
        assertEquals(expReads, misses);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMissesOnEmptyCache() throws Exception {
        GridCache<Integer, Integer> cache = grid(0).cache(null);

        assertEquals("Expected 0 read", 0, cache.metrics().getCacheGets());
        assertEquals("Expected 0 miss", 0, cache.metrics().getCacheMisses());

        Integer key =  null;

        for (int i = 0; i < 1000; i++) {
            if (cache.affinity().isPrimary(grid(0).localNode(), i)) {
                key = i;

                break;
            }
        }

        assertNotNull(key);

        cache.get(key);

        assertEquals("Expected 1 read", 1, cache.metrics().getCacheGets());
        assertEquals("Expected 1 miss", 1, cache.metrics().getCacheMisses());

        cache.put(key, key); // +1 read, +1 miss.

        cache.get(key);

        assertEquals("Expected 1 write", 1, cache.metrics().getCachePuts());
        assertEquals("Expected 3 reads", 3, cache.metrics().getCacheGets());
        assertEquals("Expected 2 misses", 2, cache.metrics().getCacheMisses());
        assertEquals("Expected 1 hit", 1, cache.metrics().getCacheHits());
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoves() throws Exception {
        GridCache<Integer, Integer> cache = grid(0).cache(null);

        cache.put(1, 1);

        // +1 remove
        cache.remove(1);

        assertEquals(1L, cache.metrics().getCacheRemovals());
    }

    /**
     * @throws Exception If failed.
     */
    public void testManualEvictions() throws Exception {
        GridCache<Integer, Integer> cache = grid(0).cache(null);

        if (cache.configuration().getCacheMode() == CacheMode.PARTITIONED)
            return;

        cache.put(1, 1);

        cache.evict(1);

        assertEquals(0L, cache.metrics().getCacheRemovals());
        assertEquals(1L, cache.metrics().getCacheEvictions());
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxEvictions() throws Exception {
        if (grid(0).cache(null).configuration().getAtomicityMode() != CacheAtomicityMode.ATOMIC)
            checkTtl(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonTxEvictions() throws Exception {
        if (grid(0).cache(null).configuration().getAtomicityMode() == CacheAtomicityMode.ATOMIC)
            checkTtl(false);
    }

    /**
     * @param inTx
     * @throws Exception If failed.
     */
    private void checkTtl(boolean inTx) throws Exception {
        int ttl = 1000;

        final ExpiryPolicy expiry = new TouchedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, ttl));

        final GridCache<Integer, Integer> c = grid(0).cache(null);

        final Integer key = primaryKeysForCache(c, 1, 0).get(0);

        c.put(key, 1);

        Entry<Integer, Integer> entry = c.entry(key);

        assert entry != null;

        assertEquals(0, entry.timeToLive());
        assertEquals(0, entry.expirationTime());
        assertEquals(0, grid(0).cache(null).metrics().getCacheEvictions());

        long startTime = System.currentTimeMillis();

        if (inTx) {
            // Rollback transaction for the first time.
            IgniteTx tx = grid(0).transactions().txStart();

            try {
                grid(0).jcache(null).withExpiryPolicy(expiry).put(key, 1);
            }
            finally {
                tx.rollback();
            }

            assertEquals(0, entry.timeToLive());
            assertEquals(0, entry.expirationTime());
        }

        // Now commit transaction and check that ttl and expire time have been saved.
        IgniteTx tx = inTx ? c.txStart() : null;

        try {
            grid(0).jcache(null).withExpiryPolicy(expiry).put(key, 1);
        }
        finally {
            if (tx != null)
                tx.commit();
        }

        long[] expireTimes = new long[gridCount()];

        for (int i = 0; i < gridCount(); i++) {
            Entry<Object, Object> curEntry = grid(i).cache(null).entry(key);

            if (curEntry.primary() || curEntry.backup()) {
                assertEquals(ttl, curEntry.timeToLive());

                assert curEntry.expirationTime() > startTime;

                expireTimes[i] = curEntry.expirationTime();
            }
        }

        // One more update from the same cache entry to ensure that expire time is shifted forward.
        IgniteUtils.sleep(100);

        tx = inTx ? c.txStart() : null;

        try {
            grid(0).jcache(null).withExpiryPolicy(expiry).put(key, 2);
        }
        finally {
            if (tx != null)
                tx.commit();
        }

        for (int i = 0; i < gridCount(); i++) {
            Entry<Object, Object> curEntry = grid(i).cache(null).entry(key);

            if (curEntry.primary() || curEntry.backup()) {
                assertEquals(ttl, curEntry.timeToLive());

                assert curEntry.expirationTime() > expireTimes[i];

                expireTimes[i] = curEntry.expirationTime();
            }
        }

        // And one more direct update to ensure that expire time is shifted forward.
        IgniteUtils.sleep(100);

        assertEquals(0, grid(0).cache(null).metrics().getCacheEvictions());

        tx = inTx ? c.txStart() : null;

        try {
            grid(0).jcache(null).withExpiryPolicy(expiry).put(key, 3);
        }
        finally {
            if (tx != null)
                tx.commit();
        }

        for (int i = 0; i < gridCount(); i++) {
            Entry<Object, Object> curEntry = grid(i).cache(null).entry(key);

            if (curEntry.primary() || curEntry.backup()) {
                assertEquals(ttl, curEntry.timeToLive());

                assert curEntry.expirationTime() > expireTimes[i];

                expireTimes[i] = curEntry.expirationTime();
            }
        }

        // And one more update to ensure that ttl is not changed and expire time is not shifted forward.
        IgniteUtils.sleep(100);

        assertEquals(0, grid(0).cache(null).metrics().getCacheEvictions());

        log.info("Put 4");

        tx = inTx ? c.txStart() : null;

        try {
            grid(0).jcache(null).put(key, 4);
        }
        finally {
            if (tx != null)
                tx.commit();
        }

        log.info("Put 4 done");

        for (int i = 0; i < gridCount(); i++) {
            Entry<Object, Object> curEntry = grid(i).cache(null).entry(key);

            if (curEntry.primary() || curEntry.backup()) {
                assertEquals(ttl, curEntry.timeToLive());
                assertEquals(expireTimes[i], curEntry.expirationTime());
            }
        }

        assertEquals(0, grid(0).cache(null).metrics().getCacheEvictions());

        // Avoid reloading from store.
        map.remove(key);

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicateX() {
            @SuppressWarnings("unchecked")
            @Override
            public boolean applyx() throws IgniteCheckedException {
                try {
                    if (c.get(key) != null)
                        return false;

                    // Get "cache" field from GridCacheProxyImpl.
                    GridCacheAdapter c0 = GridTestUtils.getFieldValue(c, "cache");

                    if (!c0.context().deferredDelete()) {
                        GridCacheEntryEx e0 = c0.peekEx(key);

                        return e0 == null || (e0.rawGet() == null && e0.valueBytes() == null);
                    } else
                        return true;
                } catch (GridCacheEntryRemovedException e) {
                    throw new RuntimeException(e);
                }
            }
        }, Math.min(ttl * 10, getTestTimeout())));

        // Ensure that old TTL and expire time are not longer "visible".
        entry = c.entry(key);

        assertEquals(0, entry.timeToLive());
        assertEquals(0, entry.expirationTime());

        // Ensure that next update will not pick old expire time.

        tx = inTx ? c.txStart() : null;

        try {
            entry.set(10);
        }
        finally {
            if (tx != null)
                tx.commit();
        }

        IgniteUtils.sleep(2000);

        entry = c.entry(key);

        assertEquals((Integer)10, entry.get());

        assertEquals(0, entry.timeToLive());
        assertEquals(0, entry.expirationTime());

        if (c.configuration().getCacheMode() != CacheMode.PARTITIONED && inTx)
            assertEquals(1, grid(0).cache(null).metrics().getCacheEvictions());
    }

    /**
     * @param cache Cache.
     * @param cnt Keys count.
     * @param startFrom Start value for keys search.
     * @return Collection of keys for which given cache is primary.
     * @throws IgniteCheckedException If failed.
     */
    protected List<Integer> primaryKeysForCache(CacheProjection<Integer, Integer> cache, int cnt, int startFrom)
            throws IgniteCheckedException {
        List<Integer> found = new ArrayList<>(cnt);

        for (int i = startFrom; i < startFrom + 100_000; i++) {
            if (cache.entry(i).primary()) {
                found.add(i);

                if (found.size() == cnt)
                    return found;
            }
        }

        throw new IgniteCheckedException("Unable to find " + cnt + " keys as primary for cache.");
    }
}
