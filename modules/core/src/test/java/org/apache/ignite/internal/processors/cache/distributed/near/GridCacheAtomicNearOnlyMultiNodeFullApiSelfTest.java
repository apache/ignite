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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;

/**
 * Tests NEAR_ONLY cache.
 */
public class GridCacheAtomicNearOnlyMultiNodeFullApiSelfTest extends GridCacheNearOnlyMultiNodeFullApiSelfTest {
    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(igniteInstanceName);

        cfg.setNearConfiguration(null);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected boolean nearEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return ATOMIC;
    }

    /** {@inheritDoc} */
    @Override protected boolean lockingEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected boolean txEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (int i = 0; i < gridCount(); i++)
            grid(i).cache(DEFAULT_CACHE_NAME).removeAll();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testClear() throws Exception {
        IgniteCache<String, Integer> nearCache = jcache();
        IgniteCache<String, Integer> primary = fullCache();

        Collection<String> keys = primaryKeysForCache(primary, 3);

        Map<String, Integer> vals = new HashMap<>();

        int i = 0;

        for (String key : keys) {
            nearCache.put(key, i);

            vals.put(key, i);

            i++;
        }

        i = 0;

        for (String key : keys)
            assertEquals((Integer)i++, nearCache.localPeek(key, CachePeekMode.ONHEAP));

        nearCache.clear();

        for (String key : keys)
            assertNull(nearCache.localPeek(key, CachePeekMode.ONHEAP));

        for (Map.Entry<String, Integer> entry : vals.entrySet())
            nearCache.put(entry.getKey(), entry.getValue());

        i = 0;

        for (String key : keys)
            assertEquals((Integer)i++, nearCache.localPeek(key, CachePeekMode.ONHEAP));
    }
}
