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
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.transactions.*;

import javax.cache.expiry.*;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;

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

            g.jcache(null).removeAll();

            assert g.jcache(null).localSize() == 0;

            g.jcache(null).mxBean().clear();

            g.transactions().resetMetrics();
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        for (int i = 0; i < gridCount(); i++) {
            Ignite g = grid(i);

            g.jcache(null).getConfiguration(CacheConfiguration.class).setStatisticsEnabled(true);
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
    public void testGetAndRemoveAsyncAvgTime() throws Exception {
        IgniteCache<Object, Object> cache = grid(0).jcache(null);

        IgniteCache<Object, Object> cacheAsync = cache.withAsync();

        cache.put(1, 1);
        cache.put(2, 2);

        assertEquals(cache.metrics().getAverageRemoveTime(), 0.0, 0.0);

        cacheAsync.getAndRemove(1);

        IgniteFuture<Object> fut = cacheAsync.future();

        assertEquals(1, (int)fut.get());

        assert cache.metrics().getAverageRemoveTime() > 0;

        cacheAsync.getAndRemove(2);

        fut = cacheAsync.future();

        assertEquals(2, (int)fut.get());

        assert cache.metrics().getAverageRemoveTime() > 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveAsyncValAvgTime() throws Exception {
        IgniteCache<Object, Object> cache = grid(0).jcache(null);
        IgniteCache<Object, Object> cacheAsync = cache.withAsync();

        Integer key = 0;

        for (int i = 0; i < 1000; i++) {
            if (affinity(cache).isPrimary(grid(0).localNode(), i)) {
                key = i;

                break;
            }
        }

        assertEquals(cache.metrics().getAverageRemoveTime(), 0.0, 0.0);

        cache.put(key, key);

        cacheAsync.remove(key, key);

        IgniteFuture<Boolean> fut = cacheAsync.future();

        assertTrue(fut.get());

        assert cache.metrics().getAverageRemoveTime() >= 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveAvgTime() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).jcache(null);

        cache.put(1, 1);
        cache.put(2, 2);

        assertEquals(cache.metrics().getAverageRemoveTime(), 0.0, 0.0);

        cache.remove(1);

        float avgRmvTime = cache.metrics().getAverageRemoveTime();

        assert avgRmvTime > 0;

        cache.remove(2);

        assert cache.metrics().getAverageRemoveTime() > 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveAllAvgTime() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).jcache(null);

        cache.put(1, 1);
        cache.put(2, 2);
        cache.put(3, 3);

        assertEquals(cache.metrics().getAverageRemoveTime(), 0.0, 0.0);

        Set<Integer> keys = new HashSet<>(4, 1);
        keys.add(1);
        keys.add(2);
        keys.add(3);

        cache.removeAll(keys);

        float averageRemoveTime = cache.metrics().getAverageRemoveTime();

        assert averageRemoveTime >= 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveAllAsyncAvgTime() throws Exception {
        IgniteCache<Object, Object> cache = grid(0).jcache(null);
        IgniteCache<Object, Object> cacheAsync = cache.withAsync();

        Set<Integer> keys = new LinkedHashSet<>();

        for (int i = 0; i < 1000; i++) {
            if (affinity(cache).isPrimary(grid(0).localNode(), i)) {
                keys.add(i);

                cache.put(i, i);

                if(keys.size() == 3)
                    break;
            }
        }

        assertEquals(cache.metrics().getAverageRemoveTime(), 0.0, 0.0);

        cacheAsync.removeAll(keys);

        IgniteFuture<?> fut = cacheAsync.future();

        fut.get();

        assert cache.metrics().getAverageRemoveTime() >= 0;
    }


    /**
     * @throws Exception If failed.
     */
    public void testGetAvgTime() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).jcache(null);

        cache.put(1, 1);

        assertEquals(0.0, cache.metrics().getAverageGetTime(), 0.0);

        cache.get(1);

        float averageGetTime = cache.metrics().getAverageGetTime();

        assert averageGetTime > 0;

        cache.get(2);

        assert cache.metrics().getAverageGetTime() > 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAllAvgTime() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).jcache(null);

        assertEquals(0.0, cache.metrics().getAverageGetTime(), 0.0);

        cache.put(1, 1);
        cache.put(2, 2);
        cache.put(3, 3);

        assertEquals(0.0, cache.metrics().getAverageGetTime(), 0.0);

        Set<Integer> keys = new TreeSet<>();
        keys.add(1);
        keys.add(2);
        keys.add(3);

        cache.getAll(keys);

        assert cache.metrics().getAverageGetTime() > 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAllAsyncAvgTime() throws Exception {
        IgniteCache<Object, Object> cache = grid(0).jcache(null);
        IgniteCache<Object, Object> cacheAsync = cache.withAsync();

        assertEquals(0.0, cache.metrics().getAverageGetTime(), 0.0);

        cache.put(1, 1);
        cache.put(2, 2);
        cache.put(3, 3);

        assertEquals(0.0, cache.metrics().getAverageGetTime(), 0.0);

        Set<Integer> keys = new TreeSet<>();
        keys.add(1);
        keys.add(2);
        keys.add(3);

        cacheAsync.getAll(keys);

        IgniteFuture<Map<Object, Object>> fut = cacheAsync.future();

        fut.get();

        TimeUnit.MILLISECONDS.sleep(100L);

        assert cache.metrics().getAverageGetTime() > 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAvgTime() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).jcache(null);

        assertEquals(0.0, cache.metrics().getAveragePutTime(), 0.0);
        assertEquals(0, cache.metrics().getCachePuts());

        cache.put(1, 1);

        float avgPutTime = cache.metrics().getAveragePutTime();

        assert avgPutTime >= 0;

        assertEquals(1, cache.metrics().getCachePuts());

        cache.put(2, 2);

        assert cache.metrics().getAveragePutTime() >= 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAsyncAvgTime() throws Exception {
        IgniteCache<Object, Object> cache = grid(0).jcache(null);
        IgniteCache<Object, Object> cacheAsync = cache.withAsync();

        assertEquals(0.0, cache.metrics().getAveragePutTime(), 0.0);
        assertEquals(0, cache.metrics().getCachePuts());

        cacheAsync.put(1, 1);

        cacheAsync.future().get();

        TimeUnit.MILLISECONDS.sleep(100L);

        assert cache.metrics().getAveragePutTime() > 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndPutAsyncAvgTime() throws Exception {
        IgniteCache<Object, Object> cache = grid(0).jcache(null);
        IgniteCache<Object, Object> cacheAsync = cache.withAsync();

        Integer key = null;

        for (int i = 0; i < 1000; i++) {
            if (affinity(cache).isPrimary(grid(0).localNode(), i)) {
                key = i;

                break;
            }
        }

        assertEquals(0.0, cache.metrics().getAveragePutTime(), 0.0);
        assertEquals(0.0, cache.metrics().getAverageGetTime(), 0.0);

        cacheAsync.getAndPut(key, key);

        IgniteFuture<?> fut = cacheAsync.future();

        fut.get();

        TimeUnit.MILLISECONDS.sleep(100L);

        assert cache.metrics().getAveragePutTime() > 0;
        assert cache.metrics().getAverageGetTime() > 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutIfAbsentAsyncAvgTime() throws Exception {
        IgniteCache<Object, Object> cache = grid(0).jcache(null);
        IgniteCache<Object, Object> cacheAsync = cache.withAsync();

        Integer key = null;

        for (int i = 0; i < 1000; i++) {
            if (affinity(cache).isPrimary(grid(0).localNode(), i)) {
                key = i;

                break;
            }
        }

        assertEquals(0.0f, cache.metrics().getAveragePutTime());

        cacheAsync.putIfAbsent(key, key);

        IgniteFuture<Boolean> fut = cacheAsync.future();

        fut.get();

        TimeUnit.MILLISECONDS.sleep(100L);

        assert cache.metrics().getAveragePutTime() > 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndPutIfAbsentAsyncAvgTime() throws Exception {
        IgniteCache<Object, Object> cache = grid(0).jcache(null);
        IgniteCache<Object, Object> cacheAsync = cache.withAsync();

        Integer key = null;

        for (int i = 0; i < 1000; i++) {
            if (affinity(cache).isPrimary(grid(0).localNode(), i)) {
                key = i;

                break;
            }
        }

        assertEquals(0.0f, cache.metrics().getAveragePutTime());

        cacheAsync.getAndPutIfAbsent(key, key);

        IgniteFuture<?> fut = cacheAsync.future();

        fut.get();

        TimeUnit.MILLISECONDS.sleep(100L);

        assert cache.metrics().getAveragePutTime() > 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllAvgTime() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).jcache(null);

        assertEquals(0.0, cache.metrics().getAveragePutTime(), 0.0);
        assertEquals(0, cache.metrics().getCachePuts());

        Map<Integer, Integer> values = new HashMap<>();

        values.put(1, 1);
        values.put(2, 2);
        values.put(3, 3);

        cache.putAll(values);

        float averagePutTime = cache.metrics().getAveragePutTime();

        assert averagePutTime >= 0;
        assertEquals(values.size(), cache.metrics().getCachePuts());
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutsReads() throws Exception {
        IgniteCache<Integer, Integer> cache0 = grid(0).jcache(null);

        int keyCnt = keyCount();

        int expReads = 0;
        int expMisses = 0;

        // Put and get a few keys.
        for (int i = 0; i < keyCnt; i++) {
            cache0.getAndPut(i, i); // +1 put

            boolean isPrimary = affinity(cache0).isPrimary(grid(0).localNode(), i);

            expReads += expectedReadsPerPut(isPrimary);
            expMisses += expectedMissesPerPut(isPrimary);

            info("Puts: " + cache0.metrics().getCachePuts());

            for (int j = 0; j < gridCount(); j++) {
                IgniteCache<Integer, Integer> cache = grid(j).jcache(null);

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
            CacheMetrics m = grid(i).jcache(null).metrics();

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
        IgniteCache<Integer, Integer> cache0 = grid(0).jcache(null);

        int keyCnt = keyCount();

        // Put and get a few keys.
        for (int i = 0; i < keyCnt; i++) {
            cache0.getAndPut(i, i); // +1 read

            info("Puts: " + cache0.metrics().getCachePuts());

            for (int j = 0; j < gridCount(); j++) {
                IgniteCache<Integer, Integer> cache = grid(j).jcache(null);

                long cacheWrites = cache.metrics().getCachePuts();

                assertEquals("Wrong cache metrics [i=" + i + ", grid=" + j + ']', i + 1, cacheWrites);
            }

            assertEquals("Wrong value for key: " + i, Integer.valueOf(i), cache0.get(i)); // +1 read
        }

        // Check metrics for the whole cache.
        for (int i = 0; i < gridCount(); i++) {
            CacheMetrics m = grid(i).jcache(null).metrics();

            assertEquals(m.getCacheHits() * 100f / m.getCacheGets(), m.getCacheHitPercentage(), 0.1f);
            assertEquals(m.getCacheMisses() * 100f / m.getCacheGets(), m.getCacheMissPercentage(), 0.1f);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMisses() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).jcache(null);

        int keyCnt = keyCount();

        int expReads = 0;

        // Get a few keys missed keys.
        for (int i = 0; i < keyCnt; i++) {
            assertNull("Value is not null for key: " + i, cache.get(i));

            if (cache.getConfiguration(CacheConfiguration.class).getCacheMode() == CacheMode.REPLICATED ||
                affinity(cache).isPrimary(grid(0).localNode(), i))
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
            CacheMetrics m = grid(i).jcache(null).metrics();

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
        IgniteCache<Integer, Integer> cache = grid(0).jcache(null);

        assertEquals("Expected 0 read", 0, cache.metrics().getCacheGets());
        assertEquals("Expected 0 miss", 0, cache.metrics().getCacheMisses());

        Integer key =  null;

        for (int i = 0; i < 1000; i++) {
            if (affinity(cache).isPrimary(grid(0).localNode(), i)) {
                key = i;

                break;
            }
        }

        assertNotNull(key);

        cache.get(key);

        assertEquals("Expected 1 read", 1, cache.metrics().getCacheGets());
        assertEquals("Expected 1 miss", 1, cache.metrics().getCacheMisses());

        cache.getAndPut(key, key); // +1 read, +1 miss.

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
        IgniteCache<Integer, Integer> cache = grid(0).jcache(null);

        cache.put(1, 1);

        // +1 remove
        cache.remove(1);

        assertEquals(1L, cache.metrics().getCacheRemovals());
    }

    /**
     * @throws Exception If failed.
     */
    public void testManualEvictions() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).jcache(null);

        if (cache.getConfiguration(CacheConfiguration.class).getCacheMode() == CacheMode.PARTITIONED)
            return;

        cache.put(1, 1);

        cache.localEvict(Collections.singleton(1));

        assertEquals(0L, cache.metrics().getCacheRemovals());
        assertEquals(1L, cache.metrics().getCacheEvictions());
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxEvictions() throws Exception {
        if (grid(0).jcache(null).getConfiguration(CacheConfiguration.class).getAtomicityMode() != CacheAtomicityMode.ATOMIC)
            checkTtl(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonTxEvictions() throws Exception {
        if (grid(0).jcache(null).getConfiguration(CacheConfiguration.class).getAtomicityMode() == CacheAtomicityMode.ATOMIC)
            checkTtl(false);
    }

    /**
     * @param inTx {@code true} for tx.
     * @throws Exception If failed.
     */
    private void checkTtl(boolean inTx) throws Exception {
        int ttl = 1000;

        final ExpiryPolicy expiry = new TouchedExpiryPolicy(new Duration(MILLISECONDS, ttl));

        final IgniteCache<Integer, Integer> c = grid(0).jcache(null);

        final Integer key = primaryKeys(jcache(0), 1, 0).get(0);

        c.put(key, 1);

        GridCacheAdapter<Object, Object> c0 = ((IgniteKernal)grid(0)).internalCache();

        if (c0.isNear())
            c0 = c0.context().near().dht();

        GridCacheEntryEx entry = c0.entryEx(key);

        assert entry != null;

        assertEquals(0, entry.ttl());
        assertEquals(0, entry.expireTime());

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

            entry = ((IgniteKernal)grid(0)).internalCache().entryEx(key);

            assertEquals(0, entry.ttl());
            assertEquals(0, entry.expireTime());
        }

        // Now commit transaction and check that ttl and expire time have been saved.
        IgniteTx tx = inTx ? grid(0).transactions().txStart() : null;

        try {
            grid(0).jcache(null).withExpiryPolicy(expiry).put(key, 1);
        }
        finally {
            if (tx != null)
                tx.commit();
        }

        long[] expireTimes = new long[gridCount()];

        for (int i = 0; i < gridCount(); i++) {
            if (grid(i).affinity(null).isPrimaryOrBackup(grid(i).localNode(), key)) {
                c0 = ((IgniteKernal)grid(i)).internalCache();

                if (c0.isNear())
                    c0 = c0.context().near().dht();

                GridCacheEntryEx<Object, Object> curEntry = c0.peekEx(key);

                assertEquals(ttl, curEntry.ttl());

                assert curEntry.expireTime() > startTime;

                expireTimes[i] = curEntry.expireTime();
            }
        }

        // One more update from the same cache entry to ensure that expire time is shifted forward.
        U.sleep(100);

        tx = inTx ? grid(0).transactions().txStart() : null;

        try {
            grid(0).jcache(null).withExpiryPolicy(expiry).put(key, 2);
        }
        finally {
            if (tx != null)
                tx.commit();
        }

        for (int i = 0; i < gridCount(); i++) {
            if (grid(i).affinity(null).isPrimaryOrBackup(grid(i).localNode(), key)) {
                c0 = ((IgniteKernal)grid(i)).internalCache();

                if (c0.isNear())
                    c0 = c0.context().near().dht();

                GridCacheEntryEx<Object, Object> curEntry = c0.peekEx(key);

                assertEquals(ttl, curEntry.ttl());

                assert curEntry.expireTime() > startTime;

                expireTimes[i] = curEntry.expireTime();
            }
        }

        // And one more direct update to ensure that expire time is shifted forward.
        U.sleep(100);

        tx = inTx ? grid(0).transactions().txStart() : null;

        try {
            grid(0).jcache(null).withExpiryPolicy(expiry).put(key, 3);
        }
        finally {
            if (tx != null)
                tx.commit();
        }

        for (int i = 0; i < gridCount(); i++) {
            if (grid(i).affinity(null).isPrimaryOrBackup(grid(i).localNode(), key)) {
                c0 = ((IgniteKernal)grid(i)).internalCache();

                if (c0.isNear())
                    c0 = c0.context().near().dht();

                GridCacheEntryEx<Object, Object> curEntry = c0.peekEx(key);

                assertEquals(ttl, curEntry.ttl());

                assert curEntry.expireTime() > startTime;

                expireTimes[i] = curEntry.expireTime();
            }
        }

        // And one more update to ensure that ttl is not changed and expire time is not shifted forward.
        U.sleep(100);

        log.info("Put 4");

        tx = inTx ? grid(0).transactions().txStart() : null;

        try {
            c.put(key, 4);
        }
        finally {
            if (tx != null)
                tx.commit();
        }

        log.info("Put 4 done");

        for (int i = 0; i < gridCount(); i++) {
            if (grid(i).affinity(null).isPrimaryOrBackup(grid(i).localNode(), key)) {
                c0 = ((IgniteKernal)grid(i)).internalCache();

                if (c0.isNear())
                    c0 = c0.context().near().dht();

                GridCacheEntryEx<Object, Object> curEntry = c0.peekEx(key);

                assertEquals(ttl, curEntry.ttl());
                assertEquals(expireTimes[i], curEntry.expireTime());
            }
        }

        // Avoid reloading from store.
        map.remove(key);

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicateX() {
            @SuppressWarnings("unchecked")
            @Override public boolean applyx() throws IgniteCheckedException {
                try {
                    if (c.get(key) != null)
                        return false;

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

        c0 = ((IgniteKernal)grid(0)).internalCache();

        if (c0.isNear())
            c0 = c0.context().near().dht();

        // Ensure that old TTL and expire time are not longer "visible".
        entry = c0.entryEx(key);

        assertEquals(0, entry.ttl());
        assertEquals(0, entry.expireTime());
    }
}
