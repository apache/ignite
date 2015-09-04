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

package org.apache.ignite.internal.processors.cache.eviction.random;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.eviction.random.RandomEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Random eviction policy cache size test.
 */
public class RandomEvictionPolicyCacheSizeSelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 3;

    /** Keys count. */
    private static final int KEYS_CNT = 50;

    /** Policy max size. */
    private static final int PLC_MAX_SIZE = 10;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = defaultCacheConfiguration();
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setNearConfiguration(null);
        ccfg.setEvictionPolicy(new RandomEvictionPolicy(PLC_MAX_SIZE));

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSize() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(null);

        for (int i = 0; i < KEYS_CNT; i++)
            cache.put(i, i);

        // Ensure that all entries accessed without data races and cache size will correct
        for (int i = 0; i < KEYS_CNT; i++)
            cache.get(i);

        assertEquals(PLC_MAX_SIZE * GRID_CNT, cache.size());
    }
}