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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.lang.IgniteFuture;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
public class IgniteCacheManyAsyncOperationsTest extends IgniteCacheAbstractTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
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
        return null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (gridName.equals(getTestGridName(2)))
            cfg.setClientMode(true);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testManyAsyncOperations() throws Exception {
        try (Ignite client = startGrid(gridCount())) {
            assertTrue(client.configuration().isClientMode());

            IgniteCache<Object, Object> cache = client.cache(null).withAsync();

            final int ASYNC_OPS = cache.getConfiguration(CacheConfiguration.class).getMaxConcurrentAsyncOperations();

            log.info("Number of async operations: " + ASYNC_OPS);

            Map<Integer, byte[]> map = new HashMap<>();

            for (int i = 0; i < 100; i++)
                map.put(i, new byte[128]);

            for (int iter = 0; iter < 3; iter++) {
                log.info("Iteration: " + iter);

                List<IgniteFuture<?>> futs = new ArrayList<>(ASYNC_OPS);

                for (int i = 0; i < ASYNC_OPS; i++) {
                    cache.putAll(map);

                    futs.add(cache.future());

                    if (i % 50 == 0)
                        log.info("Created futures: " + (i + 1));
                }

                for (int i = 0; i < ASYNC_OPS; i++) {
                    IgniteFuture<?> fut = futs.get(i);

                    fut.get();

                    if (i % 50 == 0)
                        log.info("Done: " + (i + 1));
                }
            }
        }
    }
}