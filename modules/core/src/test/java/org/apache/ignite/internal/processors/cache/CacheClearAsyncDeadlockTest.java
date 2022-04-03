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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Check that there is no deadlock when clear cache operation is called and management pool is small enough.
 */
public class CacheClearAsyncDeadlockTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setManagementThreadPoolSize(1)
            .setCacheConfiguration(
                new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
                    .setBackups(2)
                    .setNearConfiguration(
                        new NearCacheConfiguration<>()
                    )
            );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void test() throws Exception {
        Ignite ignite = startGridsMultiThreaded(3);

        IgniteCache<Integer, Integer> cache = grid(1).getOrCreateCache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 1000; i++)
            cache.put(i, i);

        Ignite client0 = startClientGrid(3);

        IgniteCache<Integer, Integer> clientCache0 = client0.getOrCreateCache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 1000; i++)
            clientCache0.get(i);

        Ignite client1 = startClientGrid(4);

        IgniteCache<Integer, Integer> clientCache1 = client1.getOrCreateCache(DEFAULT_CACHE_NAME);

        IgniteFuture<Void> fut = clientCache1.clearAsync();

        fut.get(10_000);

        assertTrue(fut.isDone());
    }
}
