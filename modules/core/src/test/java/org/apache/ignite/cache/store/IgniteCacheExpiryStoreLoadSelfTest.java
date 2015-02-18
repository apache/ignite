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

package org.apache.ignite.cache.store;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.configuration.*;
import javax.cache.expiry.*;
import javax.cache.integration.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheMode.*;

/**
 * Test check that cache values removes from cache on expiry.
 */
public class IgniteCacheExpiryStoreLoadSelfTest extends GridCacheAbstractSelfTest {
    /** Expected time to live in milliseconds. */
    private static final int TIME_TO_LIVE = 1000;

    /** Additional time to wait expiry process in milliseconds. */
    private static final int WAIT_TIME = 500;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setCacheStoreFactory(new FactoryBuilder.SingletonFactory(new TestStore()));
        cfg.setReadThrough(true);
        cfg.setWriteThrough(true);
        cfg.setLoadPreviousValue(true);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCacheWithExpiry() throws Exception {
        checkLoad(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCacheWithExpiryAsync() throws Exception {
        checkLoad(true);
    }

    /**
     * @param async If {@code true} uses asynchronous method.
     * @throws Exception If failed.
     */
    private void checkLoad(boolean async) throws Exception {
        IgniteCache<String, Integer> cache = jcache(0)
           .withExpiryPolicy(new CreatedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, TIME_TO_LIVE)));

         List<Integer> keys = new ArrayList<>();

        keys.add(primaryKey(jcache(0)));
        keys.add(primaryKey(jcache(1)));
        keys.add(primaryKey(jcache(2)));

        if (async) {
            IgniteCache<String, Integer> asyncCache = cache.withAsync();

            asyncCache.loadCache(null, keys.toArray(new Integer[3]));

            asyncCache.future().get();
        }
        else
            cache.loadCache(null, keys.toArray(new Integer[3]));

        assertEquals(3, cache.size(CachePeekMode.PRIMARY));

        Thread.sleep(TIME_TO_LIVE + WAIT_TIME);

        assertEquals(0, cache.size(CachePeekMode.PRIMARY));
    }

    /**
     * Test cache store.
     */
    private static class TestStore implements CacheStore<Integer, Integer> {
        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Integer, Integer> clo,
            @Nullable Object... args) throws CacheLoaderException {
            assertNotNull(args);
            assertTrue(args.length > 0);

            for (Object arg : args) {
                Integer k = (Integer)arg;

                clo.apply(k, k);
            }
        }

        /** {@inheritDoc} */
        @Override public void txEnd(boolean commit) throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Integer load(Integer key) throws CacheLoaderException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Map<Integer, Integer> loadAll(Iterable<? extends Integer> keys) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Integer, ? extends Integer> entry) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void writeAll(Collection<Cache.Entry<? extends Integer, ? extends Integer>> entries) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void deleteAll(Collection<?> keys) {
            // No-op.
        }
    }
}
