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

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.expiry.TouchedExpiryPolicy;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.lang.GridAbsPredicateX;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Cache metrics test.
 */
public abstract class GridCacheAbstractMetricsSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int KEY_CNT = 500;

    /** Entry processor, performing removal. */
    private final CacheEntryProcessor<Integer, Integer, Object> removingProcessor =
        new CacheEntryProcessor<Integer, Integer, Object>() {
            @Override
            public Object process(
                    MutableEntry<Integer, Integer> entry,
                    Object... arguments
            ) throws EntryProcessorException {

                entry.remove();

                return null;
            }
        };

    /** Entry processor, performing reading. */
    private final CacheEntryProcessor<Integer, Integer, Object> readingProcessor =
        new CacheEntryProcessor<Integer, Integer, Object>() {
            @Override
            public Object process(
                    MutableEntry<Integer, Integer> entry,
                    Object... arguments
            ) throws EntryProcessorException {

                entry.getValue();

                return null;
            }
        };

    /** Entry processor, performing updating. */
    private final CacheEntryProcessor<Integer, Integer, Object> updatingProcessor =
        new CacheEntryProcessor<Integer, Integer, Object>() {
            @Override
            public Object process(
                    MutableEntry<Integer, Integer> entry,
                    Object... arguments
            ) throws EntryProcessorException {

                entry.setValue(1);

                return null;
            }
        };

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
        return 1;
    }

    /**
     * Gets number of missed per "put" operation.
     *
     * @param isPrimary {@code true} if local node is primary for current key, {@code false} otherwise.
     * @return Expected number of misses.
     */
    protected int expectedMissesPerPut(boolean isPrimary) {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        for (int i = 0; i < gridCount(); i++) {
            Ignite g = grid(i);

            g.cache(DEFAULT_CACHE_NAME).removeAll();

            assert g.cache(DEFAULT_CACHE_NAME).localSize() == 0;
        }

        for (int i = 0; i < gridCount(); i++) {
            Ignite g = grid(i);

            g.cache(DEFAULT_CACHE_NAME).localMxBean().clear();

            g.transactions().resetMetrics();
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        for (int i = 0; i < gridCount(); i++) {
            Ignite g = grid(i);

            IgniteCache cache = g.cache(DEFAULT_CACHE_NAME);

            cache.enableStatistics(true);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetMetricsDisable() throws Exception {
        // Disable statistics.
        for (int i = 0; i < gridCount(); i++) {
            Ignite g = grid(i);

            IgniteCache cache = g.cache(DEFAULT_CACHE_NAME);

            cache.enableStatistics(false);
        }

        IgniteCache<Object, Object> jcache = grid(0).cache(DEFAULT_CACHE_NAME);

        // Write to cache.
        for (int i = 0; i < KEY_CNT; i++)
            jcache.put(i, i);

        // Invoke update on cache.
        for (int i = 0; i < KEY_CNT; i++)
            jcache.invoke(i, new CacheEntryProcessor<Object, Object, Object>() {
                @Override
                public Object process(MutableEntry<Object, Object> entry,
                                      Object... arguments) throws EntryProcessorException {

                    Object key = entry.getKey();

                    entry.setValue(key);

                    return null;
                }
            });

        // Read-only invoke on cache.
        for (int i = 0; i < KEY_CNT; i++)
            jcache.invoke(i, new CacheEntryProcessor<Object, Object, Object>() {
                @Override
                public Object process(MutableEntry<Object, Object> entry,
                                      Object... arguments) throws EntryProcessorException {

                    entry.getKey();

                    return null;
                }
            });

        // Remove invoke on cache.
        for (int i = 0; i < KEY_CNT; i++)
            jcache.invoke(i, new CacheEntryProcessor<Object, Object, Object>() {
                @Override
                public Object process(MutableEntry<Object, Object> entry,
                                      Object... arguments) throws EntryProcessorException {

                    entry.remove();

                    return null;
                }
            });

        // Get from cache.
        for (int i = 0; i < KEY_CNT; i++)
            jcache.get(i);

        // Remove from cache.
        for (int i = 0; i < KEY_CNT; i++)
            jcache.remove(i);

        // Assert that statistics is clear.
        for (int i = 0; i < gridCount(); i++) {
            CacheMetrics m = grid(i).cache(DEFAULT_CACHE_NAME).localMetrics();

            assertEquals(m.getCacheGets(), 0);
            assertEquals(m.getCachePuts(), 0);
            assertEquals(m.getCacheRemovals(), 0);
            assertEquals(m.getCacheHits(), 0);
            assertEquals(m.getCacheMisses(), 0);
            assertEquals(m.getAverageGetTime(), 0f);
            assertEquals(m.getAverageRemoveTime(), 0f);
            assertEquals(m.getAveragePutTime(), 0f);
            assertEquals(m.getAverageTxCommitTime(), 0f);
            assertEquals(m.getAverageTxRollbackTime(), 0f);

            assertEquals(m.getEntryProcessorPuts(), 0);
            assertEquals(m.getEntryProcessorRemovals(), 0);
            assertEquals(m.getEntryProcessorReadOnlyInvocations(), 0);
            assertEquals(m.getEntryProcessorMinInvocationTime(), 0f);
            assertEquals(m.getEntryProcessorMaxInvocationTime(), 0f);
            assertEquals(m.getEntryProcessorAverageInvocationTime(), 0f);
            assertEquals(m.getEntryProcessorInvocations(), 0);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetMetricsSnapshot() throws Exception {
        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        assertNotSame("Method metrics() should return snapshot.", cache.localMetrics(), cache.localMetrics());
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndRemoveAsyncAvgTime() throws Exception {
        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < KEY_CNT; i++)
            cache.put(i, i);

        assertEquals(0.0, cache.localMetrics().getAverageRemoveTime(), 0.0);

        for (int i = 0; i < KEY_CNT; i++)
            cache.getAndRemoveAsync(i).get();

        assert cache.localMetrics().getAverageRemoveTime() > 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveAsyncValAvgTime() throws Exception {
        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        Integer key = 0;

        for (int i = 0; i < 1000; i++) {
            if (affinity(cache).isPrimary(grid(0).localNode(), i)) {
                key = i;

                break;
            }
        }

        assertEquals(cache.localMetrics().getAverageRemoveTime(), 0.0, 0.0);

        cache.put(key, key);

        IgniteFuture<Boolean> fut = cache.removeAsync(key, key);

        assertTrue(fut.get());

        assert cache.localMetrics().getAverageRemoveTime() >= 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveAvgTime() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < KEY_CNT; i++)
            cache.put(i, i);

        assertEquals(0.0, cache.localMetrics().getAverageRemoveTime(), 0.0);

        for (int i = 0; i < KEY_CNT; i++)
            cache.remove(i);

        assert cache.localMetrics().getAverageRemoveTime() > 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveAllAvgTime() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        cache.put(1, 1);
        cache.put(2, 2);
        cache.put(3, 3);

        assertEquals(0.0, cache.localMetrics().getAverageRemoveTime(), 0.0);

        Set<Integer> keys = new HashSet<>(4, 1);
        keys.add(1);
        keys.add(2);
        keys.add(3);

        cache.removeAll(keys);

        float averageRmvTime = cache.localMetrics().getAverageRemoveTime();

        assert averageRmvTime >= 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveAllAsyncAvgTime() throws Exception {
        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        Set<Integer> keys = new LinkedHashSet<>();

        for (int i = 0; i < 1000; i++) {
            if (affinity(cache).isPrimary(grid(0).localNode(), i)) {
                keys.add(i);

                cache.put(i, i);

                if(keys.size() == 3)
                    break;
            }
        }

        assertEquals(0.0, cache.localMetrics().getAverageRemoveTime(), 0.0);

        IgniteFuture<?> fut = cache.removeAllAsync(keys);

        fut.get();

        assert cache.localMetrics().getAverageRemoveTime() >= 0;
    }


    /**
     * @throws Exception If failed.
     */
    public void testGetAvgTime() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        cache.put(1, 1);

        assertEquals(0.0, cache.localMetrics().getAverageGetTime(), 0.0);

        cache.get(1);

        float averageGetTime = cache.localMetrics().getAverageGetTime();

        assert averageGetTime > 0;

        cache.get(2);

        assert cache.localMetrics().getAverageGetTime() > 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAllAvgTime() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        assertEquals(0.0, cache.localMetrics().getAverageGetTime(), 0.0);

        cache.put(1, 1);
        cache.put(2, 2);
        cache.put(3, 3);

        assertEquals(0.0, cache.localMetrics().getAverageGetTime(), 0.0);

        Set<Integer> keys = new TreeSet<>();
        keys.add(1);
        keys.add(2);
        keys.add(3);

        cache.getAll(keys);

        assert cache.localMetrics().getAverageGetTime() > 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAllAsyncAvgTime() throws Exception {
        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        assertEquals(0.0, cache.localMetrics().getAverageGetTime(), 0.0);

        cache.put(1, 1);
        cache.put(2, 2);
        cache.put(3, 3);

        assertEquals(0.0, cache.localMetrics().getAverageGetTime(), 0.0);

        Set<Integer> keys = new TreeSet<>();
        keys.add(1);
        keys.add(2);
        keys.add(3);

        IgniteFuture<Map<Object, Object>> fut = cache.getAllAsync(keys);

        fut.get();

        TimeUnit.MILLISECONDS.sleep(100L);

        assert cache.localMetrics().getAverageGetTime() > 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAvgTime() throws Exception {
        final IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        assertEquals(0.0, cache.localMetrics().getAveragePutTime(), 0.0);
        assertEquals(0, cache.localMetrics().getCachePuts());

        for (int i = 0; i < KEY_CNT; i++)
            cache.put(i, i);

        assert cache.localMetrics().getAveragePutTime() > 0;

        assertEquals(KEY_CNT, cache.localMetrics().getCachePuts());
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAsyncAvgTime() throws Exception {
        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        assertEquals(0.0, cache.localMetrics().getAveragePutTime(), 0.0);
        assertEquals(0, cache.localMetrics().getCachePuts());

        cache.putAsync(1, 1).get();

        TimeUnit.MILLISECONDS.sleep(100L);

        assert cache.localMetrics().getAveragePutTime() > 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndPutAsyncAvgTime() throws Exception {
        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        Integer key = null;

        for (int i = 0; i < 1000; i++) {
            if (affinity(cache).isPrimary(grid(0).localNode(), i)) {
                key = i;

                break;
            }
        }

        assertEquals(0.0, cache.localMetrics().getAveragePutTime(), 0.0);
        assertEquals(0.0, cache.localMetrics().getAverageGetTime(), 0.0);

        cache.getAndPutAsync(key, key).get();

        TimeUnit.MILLISECONDS.sleep(100L);

        assert cache.localMetrics().getAveragePutTime() > 0;
        assert cache.localMetrics().getAverageGetTime() > 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutIfAbsentAsyncAvgTime() throws Exception {
        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        Integer key = null;

        for (int i = 0; i < 1000; i++) {
            if (affinity(cache).isPrimary(grid(0).localNode(), i)) {
                key = i;

                break;
            }
        }

        assertEquals(0.0f, cache.localMetrics().getAveragePutTime());

        cache.putIfAbsentAsync(key, key).get();

        TimeUnit.MILLISECONDS.sleep(100L);

        assert cache.localMetrics().getAveragePutTime() > 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndPutIfAbsentAsyncAvgTime() throws Exception {
        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        Integer key = null;

        for (int i = 0; i < 1000; i++) {
            if (affinity(cache).isPrimary(grid(0).localNode(), i)) {
                key = i;

                break;
            }
        }

        assertEquals(0.0f, cache.localMetrics().getAveragePutTime());

        cache.getAndPutIfAbsentAsync(key, key).get();

        TimeUnit.MILLISECONDS.sleep(100L);

        assert cache.localMetrics().getAveragePutTime() > 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllAvgTime() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        assertEquals(0.0, cache.localMetrics().getAveragePutTime(), 0.0);
        assertEquals(0, cache.localMetrics().getCachePuts());

        Map<Integer, Integer> values = new HashMap<>();

        values.put(1, 1);
        values.put(2, 2);
        values.put(3, 3);

        cache.putAll(values);

        float averagePutTime = cache.localMetrics().getAveragePutTime();

        assert averagePutTime >= 0;
        assertEquals(values.size(), cache.localMetrics().getCachePuts());
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutsReads() throws Exception {
        IgniteCache<Integer, Integer> cache0 = grid(0).cache(DEFAULT_CACHE_NAME);

        int keyCnt = keyCount();

        int expReads = 0;
        int expMisses = 0;

        // Put and get a few keys.
        for (int i = 0; i < keyCnt; i++) {
            cache0.getAndPut(i, i); // +1 put

            boolean isPrimary = affinity(cache0).isPrimary(grid(0).localNode(), i);

            expReads += expectedReadsPerPut(isPrimary);
            expMisses += expectedMissesPerPut(isPrimary);

            info("Puts: " + cache0.localMetrics().getCachePuts());

            for (int j = 0; j < gridCount(); j++) {
                IgniteCache<Integer, Integer> cache = grid(j).cache(DEFAULT_CACHE_NAME);

                int cacheWrites = (int)cache.localMetrics().getCachePuts();

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
            CacheMetrics m = grid(i).cache(DEFAULT_CACHE_NAME).localMetrics();

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
        IgniteCache<Integer, Integer> cache0 = grid(0).cache(DEFAULT_CACHE_NAME);

        final int keyCnt = keyCount();

        // Put and get a few keys.
        for (int i = 0; i < keyCnt; i++) {
            cache0.getAndPut(i, i); // +1 read

            info("Puts: " + cache0.localMetrics().getCachePuts());

            for (int j = 0; j < gridCount(); j++) {
                IgniteCache<Integer, Integer> cache = grid(j).cache(DEFAULT_CACHE_NAME);

                long cacheWrites = cache.localMetrics().getCachePuts();

                assertEquals("Wrong cache metrics [i=" + i + ", grid=" + j + ']', i + 1, cacheWrites);
            }

            assertEquals("Wrong value for key: " + i, Integer.valueOf(i), cache0.get(i)); // +1 read
        }

        // Check metrics for the whole cache.
        for (int i = 0; i < gridCount(); i++) {
            CacheMetrics m = grid(i).cache(DEFAULT_CACHE_NAME).localMetrics();

            assertEquals(m.getCacheHits() * 100f / m.getCacheGets(), m.getCacheHitPercentage(), 0.1f);
            assertEquals(m.getCacheMisses() * 100f / m.getCacheGets(), m.getCacheMissPercentage(), 0.1f);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMisses() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

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
            CacheMetrics m = grid(i).cache(DEFAULT_CACHE_NAME).localMetrics();

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
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        assertEquals("Expected 0 read", 0, cache.localMetrics().getCacheGets());
        assertEquals("Expected 0 miss", 0, cache.localMetrics().getCacheMisses());

        Integer key = null;

        for (int i = 0; i < 1000; i++) {
            if (affinity(cache).isPrimary(grid(0).localNode(), i)) {
                key = i;

                break;
            }
        }

        assertNotNull(key);

        cache.get(key);

        assertEquals("Expected 1 read", 1, cache.localMetrics().getCacheGets());
        assertEquals("Expected 1 miss", 1, cache.localMetrics().getCacheMisses());

        cache.getAndPut(key, key); // +1 read, +1 miss.

        assertEquals("Expected 2 reads", 2, cache.localMetrics().getCacheGets());

        cache.get(key);

        assertEquals("Expected 1 write", 1, cache.localMetrics().getCachePuts());
        assertEquals("Expected 3 reads", 3, cache.localMetrics().getCacheGets());
        assertEquals("Expected 2 misses", 2, cache.localMetrics().getCacheMisses());
        assertEquals("Expected 1 hit", 1, cache.localMetrics().getCacheHits());
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoves() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        cache.put(1, 1);

        // +1 remove
        cache.remove(1);

        assertEquals(1L, cache.localMetrics().getCacheRemovals());
    }

    /**
     * Test {@link CacheMetrics#getSize()} and {@link CacheMetrics#getCacheSize()} work equally.
     *
     * @throws Exception If failed.
     */
    public void testCacheSizeWorksAsSize() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        assertEquals(cache.metrics().getSize(), cache.metrics().getCacheSize());

        for (int i = 0; i < KEY_CNT; i++) {
            cache.put(i, i);

            CacheMetrics metrics = cache.metrics();

            assertEquals(metrics.getSize(), metrics.getCacheSize());

            CacheMetrics localMetrics = cache.localMetrics();

            assertEquals(localMetrics.getSize(), localMetrics.getCacheSize());
        }

        for (int i = 0; i < KEY_CNT / 2; i++) {
            cache.remove(i, i);

            CacheMetrics metrics = cache.metrics();

            assertEquals(metrics.getSize(), metrics.getCacheSize());

            CacheMetrics localMetrics = cache.localMetrics();

            assertEquals(localMetrics.getSize(), localMetrics.getCacheSize());
        }

        cache.removeAll();

        assertEquals(cache.metrics().getSize(), cache.metrics().getCacheSize());
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxEvictions() throws Exception {
        if (grid(0).cache(DEFAULT_CACHE_NAME).getConfiguration(CacheConfiguration.class).getAtomicityMode() != CacheAtomicityMode.ATOMIC)
            checkTtl(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonTxEvictions() throws Exception {
        if (grid(0).cache(DEFAULT_CACHE_NAME).getConfiguration(CacheConfiguration.class).getAtomicityMode() == CacheAtomicityMode.ATOMIC)
            checkTtl(false);
    }

    /**
     * @param inTx {@code true} for tx.
     * @throws Exception If failed.
     */
    private void checkTtl(boolean inTx) throws Exception {
        int ttl = 1000;

        final ExpiryPolicy expiry = new TouchedExpiryPolicy(new Duration(MILLISECONDS, ttl));

        final IgniteCache<Integer, Integer> c = grid(0).cache(DEFAULT_CACHE_NAME);

        final Integer key = primaryKeys(jcache(0), 1, 0).get(0);

        c.put(key, 1);

        GridCacheAdapter<Object, Object> c0 = ((IgniteKernal)grid(0)).internalCache(DEFAULT_CACHE_NAME);

        if (c0.isNear())
            c0 = c0.context().near().dht();

        GridCacheEntryEx entry = c0.entryEx(key);

        assert entry != null;

        assertEquals(0, entry.ttl());
        assertEquals(0, entry.expireTime());

        long startTime = U.currentTimeMillis();

        if (inTx) {
            // Rollback transaction for the first time.
            Transaction tx = grid(0).transactions().txStart();

            try {
                grid(0).cache(DEFAULT_CACHE_NAME).withExpiryPolicy(expiry).put(key, 1);
            }
            finally {
                tx.rollback();
            }

            entry = ((IgniteKernal)grid(0)).internalCache(DEFAULT_CACHE_NAME).entryEx(key);

            assertEquals(0, entry.ttl());
            assertEquals(0, entry.expireTime());
        }

        // Now commit transaction and check that ttl and expire time have been saved.
        Transaction tx = inTx ? grid(0).transactions().txStart() : null;

        try {
            grid(0).cache(DEFAULT_CACHE_NAME).withExpiryPolicy(expiry).put(key, 1);
        }
        finally {
            if (tx != null)
                tx.commit();
        }

        long[] expireTimes = new long[gridCount()];

        for (int i = 0; i < gridCount(); i++) {
            if (grid(i).affinity(DEFAULT_CACHE_NAME).isPrimaryOrBackup(grid(i).localNode(), key)) {
                c0 = ((IgniteKernal)grid(i)).internalCache(DEFAULT_CACHE_NAME);

                if (c0.isNear())
                    c0 = c0.context().near().dht();

                GridCacheEntryEx curEntry = c0.entryEx(key);

                curEntry.unswap();

                assertTrue(curEntry.expireTime() >= startTime);

                expireTimes[i] = curEntry.expireTime();
            }
        }

        // One more update from the same cache entry to ensure that expire time is shifted forward.
        U.sleep(100);

        tx = inTx ? grid(0).transactions().txStart() : null;

        try {
            grid(0).cache(DEFAULT_CACHE_NAME).withExpiryPolicy(expiry).put(key, 2);
        }
        finally {
            if (tx != null)
                tx.commit();
        }

        for (int i = 0; i < gridCount(); i++) {
            if (grid(i).affinity(DEFAULT_CACHE_NAME).isPrimaryOrBackup(grid(i).localNode(), key)) {
                c0 = ((IgniteKernal)grid(i)).internalCache(DEFAULT_CACHE_NAME);

                if (c0.isNear())
                    c0 = c0.context().near().dht();

                GridCacheEntryEx curEntry = c0.entryEx(key);

                curEntry.unswap();

                assertTrue(curEntry.expireTime() >= startTime);

                expireTimes[i] = curEntry.expireTime();
            }
        }

        // And one more direct update to ensure that expire time is shifted forward.
        U.sleep(100);

        tx = inTx ? grid(0).transactions().txStart() : null;

        try {
            grid(0).cache(DEFAULT_CACHE_NAME).withExpiryPolicy(expiry).put(key, 3);
        }
        finally {
            if (tx != null)
                tx.commit();
        }

        for (int i = 0; i < gridCount(); i++) {
            if (grid(i).affinity(DEFAULT_CACHE_NAME).isPrimaryOrBackup(grid(i).localNode(), key)) {
                c0 = ((IgniteKernal)grid(i)).internalCache(DEFAULT_CACHE_NAME);

                if (c0.isNear())
                    c0 = c0.context().near().dht();

                GridCacheEntryEx curEntry = c0.entryEx(key);

                curEntry.unswap();

                assertTrue(curEntry.expireTime() >= startTime);

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
            if (grid(i).affinity(DEFAULT_CACHE_NAME).isPrimaryOrBackup(grid(i).localNode(), key)) {
                c0 = ((IgniteKernal)grid(i)).internalCache(DEFAULT_CACHE_NAME);

                if (c0.isNear())
                    c0 = c0.context().near().dht();

                GridCacheEntryEx curEntry = c0.entryEx(key);

                curEntry.unswap();

                assertEquals(expireTimes[i], curEntry.expireTime());
            }
        }

        // Avoid reloading from store.
        storeStgy.removeFromStore(key);

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicateX() {
            @SuppressWarnings("unchecked")
            @Override public boolean applyx() {
                try {
                    if (c.get(key) != null)
                        return false;

                    // Get "cache" field from GridCacheProxyImpl.
                    GridCacheAdapter c0 = cacheFromCtx(c);

                    if (!c0.context().deferredDelete()) {
                        GridCacheEntryEx e0 = c0.entryEx(key);

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

        c0 = ((IgniteKernal)grid(0)).internalCache(DEFAULT_CACHE_NAME);

        if (c0.isNear())
            c0 = c0.context().near().dht();

        // Ensure that old TTL and expire time are not longer "visible".
        entry = c0.entryEx(key);

        assertEquals(0, entry.expireTime());
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testInvocationRemovesOnEmptyCache() throws IgniteCheckedException {
        testInvocationRemoves(true);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testInvocationRemoves() throws IgniteCheckedException {
        testInvocationRemoves(false);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void testInvocationRemoves(boolean emptyCache) throws IgniteCheckedException {
        IgniteCache<Integer, Integer> cache0 = grid(0).cache(DEFAULT_CACHE_NAME);

        final Integer key = primaryKey(cache0);

        if (emptyCache)
            cache0.remove(key);
        else
            cache0.put(key, 0);

        cache0.invoke(key, removingProcessor);

        assertEquals(1, cache0.localMetrics().getEntryProcessorRemovals());

//        if (emptyCache) {
//            assertEquals(1, cache0.localMetrics().getEntryProcessorMisses());
//
//            assertEquals(100f, cache0.localMetrics().getEntryProcessorMissPercentage());
//            assertEquals(0f, cache0.localMetrics().getEntryProcessorHitPercentage());
//        }
//        else {
//            assertEquals(1, cache0.localMetrics().getEntryProcessorHits());
//
//            assertEquals(0f, cache0.localMetrics().getEntryProcessorMissPercentage());
//            assertEquals(100f, cache0.localMetrics().getEntryProcessorHitPercentage());
//        }
//
//        for (int i = 1; i < gridCount(); i++) {
//            Ignite ignite = ignite(i);
//
//            IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);
//
//            if (affinity(cache).isPrimaryOrBackup(ignite.cluster().localNode(), key))
//                assertEquals(1, cache.localMetrics().getEntryProcessorRemovals());
//        }
//
//        assertEquals(1, cache0.localMetrics().getEntryProcessorInvocations());
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testUpdateInvocationsOnEmptyCache() throws IgniteCheckedException {
        testUpdateInvocations(true);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testUpdateInvocations() throws IgniteCheckedException {
        testUpdateInvocations(false);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void testUpdateInvocations(final boolean emptyCache) throws IgniteCheckedException {
        IgniteCache<Integer, Integer> cache0 = grid(0).cache(DEFAULT_CACHE_NAME);

        final Integer key = primaryKey(jcache(0));

        if (emptyCache)
            cache0.remove(key);
        else
            cache0.put(key, 0);

        cache0.invoke(key, updatingProcessor);

        assertEquals(1, cache0.localMetrics().getEntryProcessorPuts());

        if (emptyCache) {
            assertEquals(1, cache0.localMetrics().getEntryProcessorMisses());

            assertEquals(100f, cache0.localMetrics().getEntryProcessorMissPercentage());
            assertEquals(0f, cache0.localMetrics().getEntryProcessorHitPercentage());
        }
        else {
            assertEquals(1, cache0.localMetrics().getEntryProcessorHits());

            assertEquals(0f, cache0.localMetrics().getEntryProcessorMissPercentage());
            assertEquals(100f, cache0.localMetrics().getEntryProcessorHitPercentage());
        }

        for (int i = 1; i < gridCount(); i++) {
            Ignite ignite = ignite(i);

            IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

            if (affinity(cache).isPrimaryOrBackup(ignite.cluster().localNode(), key))
                assertEquals(1, cache.localMetrics().getEntryProcessorPuts());
        }

        assertEquals(1, cache0.localMetrics().getEntryProcessorInvocations());
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testReadOnlyInvocationsOnEmptyCache() throws IgniteCheckedException {
        testReadOnlyInvocations(true);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testReadOnlyInvocations() throws IgniteCheckedException {
        testReadOnlyInvocations(false);
    }

    /**
     * @param emptyCache Empty cache.
     * @throws IgniteCheckedException If failed.
     */
    private void testReadOnlyInvocations(final boolean emptyCache) throws IgniteCheckedException {
        IgniteCache<Integer, Integer> cache0 = grid(0).cache(DEFAULT_CACHE_NAME);

        final Integer key = primaryKey(jcache(0));

        if (emptyCache)
            cache0.remove(key);
        else
            cache0.put(key, 0);

        cache0.invoke(key, readingProcessor);

        assertEquals(1, cache0.localMetrics().getEntryProcessorReadOnlyInvocations());

        if (emptyCache) {
            assertEquals(1, cache0.localMetrics().getEntryProcessorMisses());

            assertEquals(100f, cache0.localMetrics().getEntryProcessorMissPercentage());
            assertEquals(0f, cache0.localMetrics().getEntryProcessorHitPercentage());
        }
        else {
            assertEquals(1, cache0.localMetrics().getEntryProcessorHits());

            assertEquals(0f, cache0.localMetrics().getEntryProcessorMissPercentage());
            assertEquals(100f, cache0.localMetrics().getEntryProcessorHitPercentage());
        }

        for (int i = 1; i < gridCount(); i++)
            assertEquals(0, jcache(i).localMetrics().getEntryProcessorReadOnlyInvocations());

        assertEquals(1, cache0.localMetrics().getEntryProcessorInvocations());
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testInvokeAvgTime() throws IgniteCheckedException {
        IgniteCache<Integer, Integer> cache0 = grid(0).cache(DEFAULT_CACHE_NAME);

        float averageTime = cache0.localMetrics().getEntryProcessorAverageInvocationTime();

        assertEquals(0.0, averageTime, 0.001f);

        final Integer key = primaryKey(cache0);

        cache0.invoke(key, new CacheEntryProcessor<Integer, Integer, Object>() {
            @Override
            public Object process(MutableEntry<Integer, Integer> entry,
                                  Object... arguments) throws EntryProcessorException {

                entry.setValue(1);

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new EntryProcessorException(e);
                }

                return null;
            }
        });

        averageTime = cache0.localMetrics().getEntryProcessorAverageInvocationTime();

        assertTrue(averageTime > 0.0);

        float maxTime = cache0.localMetrics().getEntryProcessorMaxInvocationTime();
        float minTime = cache0.localMetrics().getEntryProcessorMinInvocationTime();

        assertTrue(maxTime > 0.0);
        assertEquals(maxTime, minTime, 0.001f);

        cache0.invoke(key, new CacheEntryProcessor<Integer, Integer, Object>() {
            @Override
            public Object process(MutableEntry<Integer, Integer> entry,
                                  Object... arguments) throws EntryProcessorException {

                entry.setValue(1);

                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    throw new EntryProcessorException(e);
                }

                return null;
            }
        });

        maxTime = cache0.localMetrics().getEntryProcessorMaxInvocationTime();
        minTime = cache0.localMetrics().getEntryProcessorMinInvocationTime();
        averageTime = cache0.localMetrics().getEntryProcessorAverageInvocationTime();

        assertTrue(maxTime > averageTime && averageTime > minTime);
        assertEquals(2, cache0.localMetrics().getEntryProcessorInvocations());
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testInvokeAsyncAvgTime() throws IgniteCheckedException {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        assertEquals(0.0, cache.localMetrics().getEntryProcessorAverageInvocationTime(), 0.001f);

        final Integer key = primaryKey(cache);

        cache.invokeAsync(key, updatingProcessor).get();

        U.sleep(100);

        assertTrue(cache.localMetrics().getEntryProcessorAverageInvocationTime() > 0.0);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testInvokeAllAvgTime() throws IgniteCheckedException {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        assertEquals(0.0, cache.localMetrics().getEntryProcessorAverageInvocationTime(), 0.001f);

        cache.invokeAll(ImmutableMap.of(0, updatingProcessor,
            1, readingProcessor,
            2, removingProcessor));

        U.sleep(100);

        assertTrue(cache.localMetrics().getEntryProcessorAverageInvocationTime() > 0.0);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testInvokeAllAsyncAvgTime() throws IgniteCheckedException {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        assertEquals(0.0, cache.localMetrics().getEntryProcessorAverageInvocationTime(), 0.001f);

        Map<Integer, EntryProcessorResult<Object>> invokeFut = cache.invokeAllAsync(
                ImmutableMap.of(0, updatingProcessor,
                        1, readingProcessor,
                        2, removingProcessor)).get();

        U.sleep(100);

        assertTrue(cache.localMetrics().getEntryProcessorAverageInvocationTime() > 0.0);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testInvokeAllMultipleKeysAvgTime() throws IgniteCheckedException {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        Set<Integer> keys = new HashSet<>();
        keys.add(1);
        keys.add(2);

        assertEquals(0.0, cache.localMetrics().getEntryProcessorAverageInvocationTime(), 0.001f);

        cache.invokeAll(keys, updatingProcessor);

        U.sleep(100);

        assertTrue(cache.localMetrics().getEntryProcessorAverageInvocationTime() > 0.0);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testInvokeAllAsyncMultipleKeysAvgTime() throws IgniteCheckedException {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        Set<Integer> keys = new HashSet<>();
        keys.add(1);
        keys.add(2);

        assertEquals(0.0, cache.localMetrics().getEntryProcessorAverageInvocationTime(), 0.001f);

        cache.invokeAllAsync(keys, updatingProcessor).get();

        U.sleep(100);

        assertTrue(cache.localMetrics().getEntryProcessorAverageInvocationTime() > 0.0);
    }
}
