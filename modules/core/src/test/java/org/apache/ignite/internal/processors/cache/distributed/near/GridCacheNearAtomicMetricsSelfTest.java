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
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.junit.Test;

/**
 * Atomic cache metrics test.
 */
public class GridCacheNearAtomicMetricsSelfTest extends GridCacheNearMetricsSelfTest {
    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(igniteInstanceName);

        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        return ccfg;
    }

    /**
     * Checks that enabled near cache does not affect metrics.
     */
    @Test
    public void testNearCachePutRemoveGetMetrics() {
        IgniteEx initiator = grid(0);

        IgniteCache<Integer, Integer> cache0 = initiator.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; ; i++) {
            if (!initiator.affinity(DEFAULT_CACHE_NAME).isPrimaryOrBackup(initiator.cluster().localNode(), i)) {
                cache0.put(i, i);

                GridCacheEntryEx nearEntry = near(cache0).peekEx(i);

                assertTrue("Near cache doesn't contain the key", nearEntry.hasValue());

                cache0.remove(i);

                nearEntry = near(cache0).peekEx(i);

                assertFalse("Near cache contains key which was deleted", nearEntry.hasValue());

                cache0.get(i);

                assertEquals(1, cache0.localMetrics().getCachePuts());
                assertEquals(1, cache0.localMetrics().getCacheRemovals());
                assertEquals(1, cache0.localMetrics().getCacheGets());

                break;
            }
        }
    }
}
