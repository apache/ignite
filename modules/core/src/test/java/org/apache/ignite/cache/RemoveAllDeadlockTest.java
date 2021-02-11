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

package org.apache.ignite.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * This test is needed for reproducing possible deadlock on concurrent {@link IgniteCache#removeAll()}
 */
public class RemoveAllDeadlockTest extends GridCommonAbstractTest {
    /** Threads number for reproducing deadlock. */
    public static final int THREADS = 4;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveAllAtomicPartitioned() throws Exception {
        startGrid(1);

        CacheConfiguration<Integer, Integer> cacheCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        cacheCfg.setCacheMode(CacheMode.PARTITIONED);

        cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        cacheCfg.setBackups(1);

        IgniteCache<Integer, Integer> cache = grid(1).getOrCreateCache(cacheCfg);

        removeAllConcurrent(cache);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveAllAtomicReplicated() throws Exception {
        startGrid(1);

        CacheConfiguration<Integer, Integer> cacheCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        cacheCfg.setCacheMode(CacheMode.REPLICATED);

        cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        cacheCfg.setBackups(0);

        IgniteCache<Integer, Integer> cache = grid(1).getOrCreateCache(cacheCfg);

        removeAllConcurrent(cache);
    }

    /**
     * @param cache Cache.
     */
    private void removeAllConcurrent(IgniteCache<Integer, Integer> cache) throws Exception {
        multithreaded(() -> {
            for (int i = 0; i < 1234; i++) {
                final int c = i % 123;

                if (c % 15 != 0) {

                    for (int j = i; j < c + i; j++)
                        cache.put(j, j * c);
                }
                else
                    cache.removeAll();
            }
        }, THREADS);
    }
}
