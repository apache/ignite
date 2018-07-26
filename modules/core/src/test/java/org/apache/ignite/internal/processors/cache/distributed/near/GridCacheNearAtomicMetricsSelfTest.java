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
import org.apache.ignite.internal.IgniteEx;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;

/**
 *
 */
public class GridCacheNearAtomicMetricsSelfTest extends GridCacheNearMetricsSelfTest {
    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return ATOMIC;
    }

    /**
     *
     */
    public void testPutRemoveGetMetrics() {
        IgniteEx initiator = grid(0);

        IgniteCache<Integer, Integer> cache0 = initiator.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; ; i++) {
            if (!initiator.affinity(DEFAULT_CACHE_NAME).isPrimaryOrBackup(initiator.cluster().localNode(), i)) {
                cache0.put(i, i);

                cache0.remove(i);

                cache0.get(i);

                assertEquals(1, cache0.localMetrics().getCachePuts());
                assertEquals(1, cache0.localMetrics().getCacheRemovals());
                assertEquals(1, cache0.localMetrics().getCacheGets());

                break;
            }
        }
    }
}
