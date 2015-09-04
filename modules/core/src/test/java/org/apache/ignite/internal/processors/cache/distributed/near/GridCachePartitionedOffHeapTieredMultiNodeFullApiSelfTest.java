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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CachePeekMode;

import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;

/**
 * Tests partitioned cache with off-heap tiered mode.
 */
public class GridCachePartitionedOffHeapTieredMultiNodeFullApiSelfTest extends GridCachePartitionedOffHeapMultiNodeFullApiSelfTest {
    /** {@inheritDoc} */
    @Override protected CacheMemoryMode memoryMode() {
        return OFFHEAP_TIERED;
    }

    /**
    * @throws Exception If failed.
    */
    public void testPut() throws Exception {
        IgniteCache<String, Integer> cache = grid(0).cache(null);

        assert gridCount() > 3;
        String key = null;

        for (int i = 0; i < 250; ++i) {
            String testKey = "key_" + i;

            if (!grid(0).affinity(null).isPrimaryOrBackup(grid(0).localNode(), testKey)) {
                key = testKey;

                break;
            }
        }

        assert key != null;

        IgniteCache<String, Integer> primaryCache = primaryCache(key);

        assertFalse(grid(0).affinity(null).isPrimary(grid(0).localNode(), key));
        assertFalse(grid(0).affinity(null).isBackup(grid(0).localNode(), key));

        primaryCache.put(key, 4); // Put from primary.

        assertNull(primaryCache.localPeek(key, CachePeekMode.ONHEAP));
        assertEquals(4, primaryCache.localPeek(key, CachePeekMode.OFFHEAP).intValue());

        cache.put(key, 5); // Put from near to add reader on primary.

        assertEquals(5, primaryCache.localPeek(key, CachePeekMode.ONHEAP).intValue());
        assertNull(primaryCache.localPeek(key, CachePeekMode.OFFHEAP));
        assertEquals(5, cache.get(key).intValue());
        assertEquals(5, map.get(key));
    }
}