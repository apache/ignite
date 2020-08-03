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

package org.apache.ignite.internal.processors.cache.version;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.cache.Cache;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.junit.Test;

/**
 * Versioned entry abstract test.
 */
public abstract class CacheVersionedEntryAbstractTest extends GridCacheAbstractSelfTest {
    /** Entries number to store in a cache. */
    private static final int ENTRIES_NUM = 500;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        Cache<Integer, String> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < ENTRIES_NUM; i++)
            cache.put(i, "value_" + i);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvoke() throws Exception {
        Cache<Integer, String> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        assertNotNull(cache.invoke(100, new EntryProcessor<Integer, String, Object>() {
            @Override public Object process(MutableEntry<Integer, String> entry, Object... args) {
                CacheEntry<Integer, String> verEntry = entry.unwrap(CacheEntry.class);

                checkVersionedEntry(verEntry);

                return verEntry.version();
            }
        }));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvokeAll() throws Exception {
        Cache<Integer, String> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        Set<Integer> keys = new HashSet<>();

        for (int i = 0; i < ENTRIES_NUM; i++)
            keys.add(i);

        Map<Integer, EntryProcessorResult<Object>> res = cache.invokeAll(keys, new EntryProcessor<Integer, String, Object>() {
            @Override public Object process(MutableEntry<Integer, String> entry, Object... args) {
                CacheEntry<Integer, String> verEntry = entry.unwrap(CacheEntry.class);

                checkVersionedEntry(verEntry);

                return verEntry.version();
            }
        });

        assertEquals(ENTRIES_NUM, res.size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalPeek() throws Exception {
        IgniteCache<Integer, String> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        Iterable<Cache.Entry<Integer, String>> entries = cache.localEntries();

        for (Cache.Entry<Integer, String> entry : entries)
            checkVersionedEntry(entry.unwrap(CacheEntry.class));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testVersionComparision() throws Exception {
        IgniteCache<Integer, String> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        CacheEntry<String, Integer> ver1 = cache.invoke(100,
            new EntryProcessor<Integer, String, CacheEntry<String, Integer>>() {
                @Override public CacheEntry<String, Integer> process(MutableEntry<Integer, String> entry,
                    Object... arguments) throws EntryProcessorException {
                        return entry.unwrap(CacheEntry.class);
                    }
            });

        cache.put(100, "new value 100");

        CacheEntry<String, Integer> ver2 = cache.invoke(100,
            new EntryProcessor<Integer, String, CacheEntry<String, Integer>>() {
                @Override public CacheEntry<String, Integer> process(MutableEntry<Integer, String> entry,
                    Object... arguments) throws EntryProcessorException {
                        return entry.unwrap(CacheEntry.class);
                    }
            });

        assert ver1.version().compareTo(ver2.version()) < 0;
    }

    /**
     * @param entry Versioned entry.
     */
    private void checkVersionedEntry(CacheEntry<Integer, String> entry) {
        assertNotNull(entry);

        assertNotNull(entry.version());

        assertNotNull(entry.getKey());
        assertNotNull(entry.getValue());
    }
}
