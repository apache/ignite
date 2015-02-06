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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.typedef.*;

import javax.cache.expiry.*;
import java.util.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheDistributionMode.*;

/**
 * Tests NEAR_ONLY cache.
 */
public class GridCacheAtomicNearOnlyMultiNodeFullApiSelfTest extends GridCacheNearOnlyMultiNodeFullApiSelfTest {
    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(gridName);

        if (cfg.getDistributionMode() != NEAR_ONLY)
            cfg.setDistributionMode(PARTITIONED_ONLY);

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
    @Override protected CacheDistributionMode distributionMode() {
        return PARTITIONED_ONLY;
    }

    @Override protected void afterTest() throws Exception {
        for (int i = 0; i < gridCount(); i++)
            grid(i).cache(null).removeAll();

        super.afterTest();
    }

    /** {@inheritDoc} */
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

    /** {@inheritDoc} */
    @Override public void testEvictExpired() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        String key = primaryKeysForCache(cache, 1).get(0);

        cache.put(key, 1);

        assertEquals((Integer)1, cache.get(key));

        long ttl = 500;

        grid(0).jcache(null).
            withExpiryPolicy(new TouchedExpiryPolicy(new Duration(MILLISECONDS, ttl))).put(key, 1);

        Thread.sleep(ttl + 100);

        // Expired entry should not be swapped.
        cache.localEvict(Collections.<String>singleton(key));

        assertNull(cache.localPeek(key, CachePeekMode.ONHEAP));

        cache.localPromote(Collections.singleton(key));

        assertNull(cache.localPeek(key, CachePeekMode.ONHEAP));

        assertTrue(cache.localSize() == 0);

        // Force reload on primary node.
        for (int i = 0; i < gridCount(); i++) {
            if (ignite(i).affinity(null).isPrimary(ignite(i).cluster().localNode(), key))
                load(jcache(i), key, true);
        }

        // Will do near get request.
        load(cache, key, true);

        assertEquals((Integer)1, cache.localPeek(key, CachePeekMode.ONHEAP));
    }
}
