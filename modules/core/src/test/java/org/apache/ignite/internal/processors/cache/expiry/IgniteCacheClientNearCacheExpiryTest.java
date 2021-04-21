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

package org.apache.ignite.internal.processors.cache.expiry;

import java.util.concurrent.TimeUnit;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheLocalConcurrentMap;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
public class IgniteCacheClientNearCacheExpiryTest extends IgniteCacheAbstractTest {
    /** */
    private static final int NODES = 3;

    /** */
    private static final int KEYS_COUNT = 2;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return NODES;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return new NearCacheConfiguration();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (igniteInstanceName.equals(getTestIgniteInstanceName(NODES - 1)))
            cfg.setClientMode(true);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExpirationOnClient() throws Exception {
        Ignite ignite = grid(NODES - 1);

        // Check size of near entries via reflection because entries is filtered for size() API call.
        IgniteEx igniteEx = (IgniteEx)ignite;

        GridCacheAdapter internalCache = igniteEx.context().cache().internalCache(DEFAULT_CACHE_NAME);

        GridCacheLocalConcurrentMap map = GridTestUtils.getFieldValue(internalCache, GridCacheAdapter.class, "map");

        assertTrue(ignite.configuration().isClientMode());

        IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

        assertTrue(((IgniteCacheProxy)cache).context().isNear());

        for (int i = 0; i < KEYS_COUNT; i++)
            cache.put(i, i);

        CreatedExpiryPolicy plc = new CreatedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, 500));

        IgniteCache<Object, Object> cacheWithExpiry = cache.withExpiryPolicy(plc);

        for (int i = KEYS_COUNT; i < KEYS_COUNT * 2; i++) {
            cacheWithExpiry.put(i, i);

            assertEquals(i, cacheWithExpiry.localPeek(i));
        }

        assertEquals(KEYS_COUNT * 2, map.publicSize(internalCache.context().cacheId()));

        U.sleep(1000);

        assertEquals(KEYS_COUNT, map.publicSize(internalCache.context().cacheId()));

        assertEquals(KEYS_COUNT, cache.size());

        for (int i = 0; i < KEYS_COUNT; i++)
            assertEquals(i, cacheWithExpiry.localPeek(i));

        for (int i = KEYS_COUNT; i < KEYS_COUNT * 2; i++)
            assertNull(cache.localPeek(i));
    }
}
