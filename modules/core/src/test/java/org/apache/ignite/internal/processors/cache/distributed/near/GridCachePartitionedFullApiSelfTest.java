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

import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractFullApiSelfTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Tests for partitioned cache.
 */
public class GridCachePartitionedFullApiSelfTest extends GridCacheAbstractFullApiSelfTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getTransactionConfiguration().setTxSerializableEnabled(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(igniteInstanceName);

        cfg.setAtomicityMode(atomicityMode());

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUpdate() throws Exception {
        if (gridCount() > 1) {
            IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

            Integer key = nearKey(cache);

            primaryCache(key, DEFAULT_CACHE_NAME).put(key, 1);

            assertEquals(1, cache.get(key));

            primaryCache(key, DEFAULT_CACHE_NAME).put(key, 2);

            if (cache.getConfiguration(CacheConfiguration.class).getNearConfiguration() != null)
                assertEquals(2, cache.localPeek(key));

            assertEquals(2, cache.get(key));

            int cnt = 0;

            for (Cache.Entry e : cache)
                cnt++;

            assertEquals(1, cnt);
        }
    }
}
