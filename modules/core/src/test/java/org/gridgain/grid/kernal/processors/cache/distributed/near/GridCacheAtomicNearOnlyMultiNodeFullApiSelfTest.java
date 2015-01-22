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

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.apache.ignite.cache.*;
import org.apache.ignite.internal.util.typedef.*;

import javax.cache.expiry.*;
import java.util.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.cache.GridCacheAtomicityMode.*;
import static org.apache.ignite.cache.GridCacheDistributionMode.*;

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
    @Override protected GridCacheAtomicityMode atomicityMode() {
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
    @Override protected GridCacheDistributionMode distributionMode() {
        return PARTITIONED_ONLY;
    }

    @Override protected void afterTest() throws Exception {
        for (int i = 0; i < gridCount(); i++)
            grid(i).cache(null).removeAll();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override public void testClear() throws Exception {
        GridCache<String, Integer> nearCache = cache();
        GridCache<String, Integer> primary = fullCache();

        Collection<String> keys = primaryKeysForCache(primary, 3);

        Map<String, Integer> vals = new HashMap<>(keys.size());

        int i = 0;

        for (String key : keys) {
            nearCache.put(key, i);

            vals.put(key, i);

            i++;
        }

        i = 0;

        for (String key : keys)
            assertEquals((Integer)i++, nearCache.peek(key));

        nearCache.clearAll();

        for (String key : keys)
            assertNull(nearCache.peek(key));

        for (Map.Entry<String, Integer> entry : vals.entrySet())
            nearCache.put(entry.getKey(), entry.getValue());

        i = 0;

        for (String key : keys)
            assertEquals((Integer)i++, nearCache.peek(key));

        String first = F.first(keys);

        nearCache.projection(gte100).clear(first);

        assertEquals((Integer)0, nearCache.peek(first));
        assertEquals(vals.get(first), primary.peek(first));

        nearCache.put(first, 101);

        nearCache.projection(gte100).clear(first);

        assertNull(nearCache.peek(first));
        assertFalse(primary.isEmpty());

        i = 0;

        for (String key : keys) {
            nearCache.put(key, i);

            vals.put(key, i);

            i++;
        }

        nearCache.put(first, 101);
        vals.put(first, 101);

        nearCache.projection(gte100).clear(first);

        for (String key : keys)
            assertEquals(vals.get(key), primary.peek(key));

        for (String key : keys) {
            if (first.equals(key))
                assertNull(nearCache.peek(key));
            else
                assertEquals(vals.get(key), nearCache.peek(key));
        }
    }

    /** {@inheritDoc} */
    @Override public void testEvictExpired() throws Exception {
        GridCache<String, Integer> cache = cache();

        String key = primaryKeysForCache(cache, 1).get(0);

        cache.put(key, 1);

        assertEquals((Integer)1, cache.get(key));

        long ttl = 500;

        grid(0).jcache(null).
            withExpiryPolicy(new TouchedExpiryPolicy(new Duration(MILLISECONDS, ttl))).put(key, 1);

        Thread.sleep(ttl + 100);

        // Expired entry should not be swapped.
        assertTrue(cache.evict(key));

        assertNull(cache.peek(key));

        assertNull(cache.promote(key));

        assertNull(cache.peek(key));

        assertTrue(cache.isEmpty());

        // Force reload on primary node.
        for (int i = 0; i < gridCount(); i++) {
            if (cache(i).entry(key).primary())
                cache(i).reload(key);
        }

        // Will do near get request.
        cache.reload(key);

        assertEquals((Integer)1, cache.peek(key));
    }
}
