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
import org.junit.Test;

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
    @Test
    public void testEntryProcessorRemove() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        cache.put(1, 20);

        int result = cache.invoke(1, new EntryProcessor<Integer, Integer, Integer>() {
            @Override public Integer process(MutableEntry<Integer, Integer> entry, Object... arguments)
                    throws EntryProcessorException {
                Integer result = entry.getValue();

                entry.remove();

                return result;
            }
        });

        assertEquals(1L, cache.localMetrics().getCachePuts());

        assertEquals(20, result);
        assertEquals(1L, cache.localMetrics().getCacheHits());
        assertEquals(100.0f, cache.localMetrics().getCacheHitPercentage());
        assertEquals(0L, cache.localMetrics().getCacheMisses());
        assertEquals(0f, cache.localMetrics().getCacheMissPercentage());
        assertEquals(1L, cache.localMetrics().getCachePuts());
        assertEquals(1L, cache.localMetrics().getCacheRemovals());
        assertEquals(0L, cache.localMetrics().getCacheEvictions());
        assert cache.localMetrics().getAveragePutTime() >= 0;
        assert cache.localMetrics().getAverageGetTime() >= 0;
        assert cache.localMetrics().getAverageRemoveTime() >= 0;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheStatistics() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        cache.put(1, 10);

        assertEquals(0, cache.localMetrics().getCacheRemovals());
        assertEquals(1, cache.localMetrics().getCachePuts());

        cache.remove(1);

        assertEquals(0, cache.localMetrics().getCacheHits());
        assertEquals(1, cache.localMetrics().getCacheRemovals());
        assertEquals(1, cache.localMetrics().getCachePuts());

        cache.remove(1);

        assertEquals(0, cache.localMetrics().getCacheHits());
        assertEquals(0, cache.localMetrics().getCacheMisses());
        assertEquals(1, cache.localMetrics().getCacheRemovals());
        assertEquals(1, cache.localMetrics().getCachePuts());

        cache.put(1, 10);
        assertTrue(cache.remove(1, 10));

        assertEquals(1, cache.localMetrics().getCacheHits());
        assertEquals(0, cache.localMetrics().getCacheMisses());
        assertEquals(2, cache.localMetrics().getCacheRemovals());
        assertEquals(2, cache.localMetrics().getCachePuts());

        assertFalse(cache.remove(1, 10));

        assertEquals(1, cache.localMetrics().getCacheHits());
        assertEquals(1, cache.localMetrics().getCacheMisses());
        assertEquals(2, cache.localMetrics().getCacheRemovals());
        assertEquals(2, cache.localMetrics().getCachePuts());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConditionReplace() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        long hitCnt = 0;
        long missCnt = 0;
        long putCnt = 0;

        boolean result = cache.replace(1, 0, 10);

        ++missCnt;
        assertFalse(result);

        assertEquals(missCnt, cache.localMetrics().getCacheMisses());
        assertEquals(hitCnt, cache.localMetrics().getCacheHits());
        assertEquals(putCnt, cache.localMetrics().getCachePuts());

        assertNull(cache.localPeek(1));

        cache.put(1, 10);
        ++putCnt;

        assertEquals(missCnt, cache.localMetrics().getCacheMisses());
        assertEquals(hitCnt, cache.localMetrics().getCacheHits());
        assertEquals(putCnt, cache.localMetrics().getCachePuts());

        assertNotNull(cache.localPeek(1));

        result = cache.replace(1, 10, 20);

        assertTrue(result);
        ++hitCnt;
        ++putCnt;

        assertEquals(missCnt, cache.localMetrics().getCacheMisses());
        assertEquals(hitCnt, cache.localMetrics().getCacheHits());
        assertEquals(putCnt, cache.localMetrics().getCachePuts());

        result = cache.replace(1, 40, 50);

        assertFalse(result);
        ++hitCnt;

        assertEquals(hitCnt, cache.localMetrics().getCacheHits());
        assertEquals(putCnt, cache.localMetrics().getCachePuts());
        assertEquals(missCnt, cache.localMetrics().getCacheMisses());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutIfAbsent() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        long hitCnt = 0;
        long missCnt = 0;
        long putCnt = 0;

        boolean result = cache.putIfAbsent(1, 1);

        ++putCnt;
        ++missCnt;

        assertTrue(result);

        assertEquals(missCnt, cache.localMetrics().getCacheMisses());
        assertEquals(hitCnt, cache.localMetrics().getCacheHits());
        assertEquals(putCnt, cache.localMetrics().getCachePuts());

        result = cache.putIfAbsent(1, 1);

        ++hitCnt;

        cache.containsKey(123);

        assertFalse(result);
        assertEquals(hitCnt, cache.localMetrics().getCacheHits());
        assertEquals(putCnt, cache.localMetrics().getCachePuts());
        assertEquals(missCnt, cache.localMetrics().getCacheMisses());
    }
}
