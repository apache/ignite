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

import java.util.HashMap;
import java.util.Map;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cluster.ClusterState;
import org.junit.Test;

/**
 * Tests that {@link IgniteCache#invoke(Object, EntryProcessor, Object...)}, {@link IgniteCache#invokeAll(Map, Object...)}
 * overloaded and async methods works fine when cluster in a {@link ClusterState#ACTIVE_READ_ONLY} mode.
 */
public class IgniteCacheInvokeClusterReadOnlyModeSelfTest extends IgniteCacheClusterReadOnlyModeAbstractTest {
    /** Cache entry processor. */
    private static final CacheEntryProcessor<Integer, Integer, Object> CACHE_ENTRY_PROCESSOR =
        (entry, args) -> updateValue(entry);

    /** Entry processor. */
    private static final EntryProcessor<Integer, Integer, Object> ENTRY_PROCESSOR = (entry, args) -> updateValue(entry);

    /** */
    @Test
    public void testInvokeCacheEntryProcessorDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.invoke(Integer.valueOf(KEY), CACHE_ENTRY_PROCESSOR));
        performActionReadOnlyExceptionExpected(cache -> cache.invoke(Integer.valueOf(UNKNOWN_KEY), CACHE_ENTRY_PROCESSOR));
    }

    /** */
    @Test
    public void testInvokeEntryProcessorDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.invoke(Integer.valueOf(KEY), ENTRY_PROCESSOR));
        performActionReadOnlyExceptionExpected(cache -> cache.invoke(Integer.valueOf(UNKNOWN_KEY), ENTRY_PROCESSOR));
    }

    /** */
    @Test
    public void testInvokeAsyncCacheEntryProcessorDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.invokeAsync(KEY, CACHE_ENTRY_PROCESSOR).get());
        performActionReadOnlyExceptionExpected(cache -> cache.invokeAsync(UNKNOWN_KEY, CACHE_ENTRY_PROCESSOR).get());
    }

    /** */
    @Test
    public void testInvokeAsyncEntryProcessorDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.invokeAsync(KEY, ENTRY_PROCESSOR).get());
        performActionReadOnlyExceptionExpected(cache -> cache.invokeAsync(UNKNOWN_KEY, ENTRY_PROCESSOR).get());
    }

    /** */
    @Test
    public void testInvokeAllCacheEntryProcessorDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.invokeAll(kvMap.keySet(), CACHE_ENTRY_PROCESSOR));
    }

    /** */
    @Test
    public void testInvokeAllEntryProcessorDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.invokeAll(kvMap.keySet(), ENTRY_PROCESSOR));
    }

    /** */
    @Test
    public void testInvokeAllCacheEntryProcessorAsyncDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.invokeAllAsync(kvMap.keySet(), CACHE_ENTRY_PROCESSOR).get());
    }

    /** */
    @Test
    public void testInvokeAllEntryProcessorAsyncDenied() {
        performActionReadOnlyExceptionExpected(cache -> cache.invokeAllAsync(kvMap.keySet(), ENTRY_PROCESSOR).get());
    }

    /** */
    @Test
    public void testInvokeAllCacheEntryProcessorsDenied() {
        Map<Integer, CacheEntryProcessor<Integer, Integer, Object>> map = new HashMap<>();

        kvMap.keySet().forEach(k -> map.put(k, CACHE_ENTRY_PROCESSOR));

        performActionReadOnlyExceptionExpected(cache -> cache.invokeAll(map));
    }

    /** */
    @Test
    public void testInvokeAllEntryProcessorsDenied() {
        Map<Integer, EntryProcessor<Integer, Integer, Object>> map = new HashMap<>();

        kvMap.keySet().forEach(k -> map.put(k, ENTRY_PROCESSOR));

        performActionReadOnlyExceptionExpected(cache -> cache.invokeAll(map));
    }

    /** */
    @Test
    public void testInvokeAllAsyncCacheEntryProcessorsDenied() {
        Map<Integer, CacheEntryProcessor<Integer, Integer, Object>> map = new HashMap<>();

        kvMap.keySet().forEach(k -> map.put(k, CACHE_ENTRY_PROCESSOR));

        performActionReadOnlyExceptionExpected(cache -> cache.invokeAllAsync(map).get());
    }

    /** */
    @Test
    public void testInvokeAllAsyncEntryProcessorsDenied() {
        Map<Integer, EntryProcessor<Integer, Integer, Object>> map = new HashMap<>();

        kvMap.keySet().forEach(k -> map.put(k, ENTRY_PROCESSOR));

        performActionReadOnlyExceptionExpected(cache -> cache.invokeAllAsync(map).get());
    }

    /** */
    private static Object updateValue(MutableEntry<Integer, Integer> entry) {
        assertEquals(kvMap.get(entry.getKey()), entry.getValue());

        entry.setValue(entry.getValue() == null ? 0 : entry.getValue() + 1);

        return null;
    }
}
