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

package org.apache.ignite.internal.processors.cache.store;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractTest;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;

/**
 * This class provides non write coalescing tests for {@link org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStore}.
 */
public class IgnteCacheClientWriteBehindStoreNonCoalescingTest extends IgniteCacheAbstractTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return ATOMIC;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected Factory<CacheStore> cacheStoreFactory() {
        return new TestIncrementStoreFactory();
    }

    /** {@inheritDoc} */
    @Override protected boolean writeBehindEnabled() { return true; }

    /** {@inheritDoc} */
    @Override protected boolean writeBehindCoalescing() { return false; }

    private static Random rnd = new Random();

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNonCoalescingIncrementing() throws Exception {
        Ignite ignite = grid(0);

        IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        assertEquals(cache.getConfiguration(CacheConfiguration.class).getCacheStoreFactory().getClass(),
            TestIncrementStoreFactory.class);

        for (int i = 0; i < CacheConfiguration.DFLT_WRITE_BEHIND_FLUSH_SIZE * 2; i++) {
            cache.put(i, i);
        }

        Collection<IgniteFuture<?>> futs = new ArrayList<>();

        for (int i = 0; i < 1000; i++)
            futs.add(updateKey(cache));

        for (IgniteFuture<?> fut : futs)
            fut.get();
    }

    /**
     * Update random key in async mode.
     *
     * @param cache Cache to use.
     * @return IgniteFuture.
     */
    private IgniteFuture<?> updateKey(IgniteCache<Integer, Integer> cache) {
        IgniteCache asyncCache = cache.withAsync();

        // Using EntryProcessor.invokeAll to increment every value in place.
        asyncCache.invoke(rnd.nextInt(100), new EntryProcessor<Integer, Integer, Object>() {
            @Override public Object process(MutableEntry<Integer, Integer> entry, Object... arguments)
                throws EntryProcessorException {
                entry.setValue(entry.getValue() + 1);

                return null;
            }
        });

        return asyncCache.future();
    }

    /**
     * Test increment store factory.
     */
    public static class TestIncrementStoreFactory implements Factory<CacheStore> {
        /** {@inheritDoc} */
        @Override public CacheStore create() {
            return new TestIncrementStore();
        }
    }

    /**
     * Test cache store to validate int value incrementing
     */
    public static class TestIncrementStore extends CacheStoreAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Object, Object> clo, Object... args) {
            for (Map.Entry<Object, Object> e : storeMap.entrySet())
                clo.apply(e.getKey(), e.getValue());
        }

        /** {@inheritDoc} */
        @Override public Object load(Object key) {
            return storeMap.get(key);
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Object, ? extends Object> entry) {
            Object oldVal = storeMap.put(entry.getKey(), entry.getValue());

            if (oldVal != null) {
                Integer oldInt = (Integer) oldVal;
                Integer newInt = (Integer)entry.getValue();

                assertTrue(
                    "newValue(" + newInt + ") != oldValue(" + oldInt + ")+1 !",
                    newInt == oldInt + 1);
            }
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            storeMap.remove(key);
        }
    }
}
