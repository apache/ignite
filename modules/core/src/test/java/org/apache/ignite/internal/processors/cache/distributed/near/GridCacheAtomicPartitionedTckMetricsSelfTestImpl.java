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

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;

/**
 * Partitioned atomic cache metrics test.
 */
public class GridCacheAtomicPartitionedTckMetricsSelfTestImpl extends GridCacheAtomicPartitionedMetricsSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testEntryProcessorRemove() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(null);

        cache.put(1, 20);

        int result = cache.invoke(1, new EntryProcessor<Integer, Integer, Integer>() {
            @Override public Integer process(MutableEntry<Integer, Integer> entry, Object... arguments)
                    throws EntryProcessorException {
                Integer result = entry.getValue();

                entry.remove();

                return result;
            }
        });

        assertEquals(1L, cache.metrics().getCachePuts());

        assertEquals(20, result);
        assertEquals(1L, cache.metrics().getCacheHits());
        assertEquals(100.0f, cache.metrics().getCacheHitPercentage());
        assertEquals(0L, cache.metrics().getCacheMisses());
        assertEquals(0f, cache.metrics().getCacheMissPercentage());
        assertEquals(1L, cache.metrics().getCachePuts());
        assertEquals(1L, cache.metrics().getCacheRemovals());
        assertEquals(0L, cache.metrics().getCacheEvictions());
        assert cache.metrics().getAveragePutTime() >= 0;
        assert cache.metrics().getAverageGetTime() >= 0;
        assert cache.metrics().getAverageRemoveTime() >= 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheStatistics() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(null);

        cache.put(1, 10);

        assertEquals(0, cache.metrics().getCacheRemovals());
        assertEquals(1, cache.metrics().getCachePuts());

        cache.remove(1);

        assertEquals(0, cache.metrics().getCacheHits());
        assertEquals(1, cache.metrics().getCacheRemovals());
        assertEquals(1, cache.metrics().getCachePuts());

        cache.remove(1);

        assertEquals(0, cache.metrics().getCacheHits());
        assertEquals(0, cache.metrics().getCacheMisses());
        assertEquals(1, cache.metrics().getCacheRemovals());
        assertEquals(1, cache.metrics().getCachePuts());

        cache.put(1, 10);
        assertTrue(cache.remove(1, 10));

        assertEquals(1, cache.metrics().getCacheHits());
        assertEquals(0, cache.metrics().getCacheMisses());
        assertEquals(2, cache.metrics().getCacheRemovals());
        assertEquals(2, cache.metrics().getCachePuts());

        assertFalse(cache.remove(1, 10));

        assertEquals(1, cache.metrics().getCacheHits());
        assertEquals(1, cache.metrics().getCacheMisses());
        assertEquals(2, cache.metrics().getCacheRemovals());
        assertEquals(2, cache.metrics().getCachePuts());
    }

    /**
     * @throws Exception If failed.
     */
    public void testConditionReplace() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(null);

        long hitCount = 0;
        long missCount = 0;
        long putCount = 0;

        boolean result = cache.replace(1, 0, 10);

        ++missCount;
        assertFalse(result);

        assertEquals(missCount, cache.metrics().getCacheMisses());
        assertEquals(hitCount, cache.metrics().getCacheHits());
        assertEquals(putCount, cache.metrics().getCachePuts());

        assertNull(cache.localPeek(1));

        cache.put(1, 10);
        ++putCount;

        assertEquals(missCount, cache.metrics().getCacheMisses());
        assertEquals(hitCount, cache.metrics().getCacheHits());
        assertEquals(putCount, cache.metrics().getCachePuts());

        assertNotNull(cache.localPeek(1));

        result = cache.replace(1, 10, 20);

        assertTrue(result);
        ++hitCount;
        ++putCount;

        assertEquals(missCount, cache.metrics().getCacheMisses());
        assertEquals(hitCount, cache.metrics().getCacheHits());
        assertEquals(putCount, cache.metrics().getCachePuts());

        result = cache.replace(1, 40, 50);

        assertFalse(result);
        ++hitCount;

        assertEquals(hitCount, cache.metrics().getCacheHits());
        assertEquals(putCount, cache.metrics().getCachePuts());
        assertEquals(missCount, cache.metrics().getCacheMisses());
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutIfAbsent() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(null);

        long hitCount = 0;
        long missCount = 0;
        long putCount = 0;

        boolean result = cache.putIfAbsent(1, 1);

        ++putCount;
        assertTrue(result);

        assertEquals(missCount, cache.metrics().getCacheMisses());
        assertEquals(hitCount, cache.metrics().getCacheHits());
        assertEquals(putCount, cache.metrics().getCachePuts());

        result = cache.putIfAbsent(1, 1);

        cache.containsKey(123);

        assertFalse(result);
        assertEquals(hitCount, cache.metrics().getCacheHits());
        assertEquals(putCount, cache.metrics().getCachePuts());
        assertEquals(missCount, cache.metrics().getCacheMisses());
    }
}