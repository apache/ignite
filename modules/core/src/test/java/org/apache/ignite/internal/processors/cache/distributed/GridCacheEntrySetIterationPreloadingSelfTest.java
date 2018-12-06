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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.Collection;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests entry wrappers after preloading happened.
 */
public class GridCacheEntrySetIterationPreloadingSelfTest extends GridCommonAbstractTest {
    /**
     * @param atomicityMode Cache atomicity mode.
     * @return Cache configuration.
     */
    @SuppressWarnings("unchecked")
    protected <K, V> CacheConfiguration<K, V> cacheConfiguration(CacheAtomicityMode atomicityMode) throws Exception {
        CacheConfiguration<K, V> ccfg = defaultCacheConfiguration();

        ccfg.setName(DEFAULT_CACHE_NAME);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setNearConfiguration(null);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid(0).destroyCache(DEFAULT_CACHE_NAME);

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testIteration() throws Exception {
        checkIteration(cacheConfiguration(CacheAtomicityMode.ATOMIC));
    }

    /**
     * @throws Exception If failed.
     */
    public void testIterationTx() throws Exception {
        checkIteration(cacheConfiguration(CacheAtomicityMode.TRANSACTIONAL));
    }


    /**
     * @throws Exception If failed.
     */
    public void testIterationMvcc() throws Exception {
        checkIteration(cacheConfiguration(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT));
    }

    /**
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    private void checkIteration(CacheConfiguration<String, Integer> ccfg) throws Exception {
        try {
            final IgniteCache<String, Integer> cache = grid(0).createCache(ccfg);

            final int entryCnt = 1000;

            for (int i = 0; i < entryCnt; i++)
                cache.put(String.valueOf(i), i);

            Collection<Cache.Entry<String, Integer>> entries = new ArrayList<>(entryCnt);

            for (Cache.Entry<String, Integer> entry : cache)
                entries.add(entry);

            startGrid(1);
            startGrid(2);
            startGrid(3);

            for (Cache.Entry<String, Integer> entry : entries)
                entry.getValue();

            for (int i = 0; i < entryCnt; i++)
                cache.remove(String.valueOf(i));
        }
        finally {
            stopGrid(3);
            stopGrid(2);
            stopGrid(1);
        }
    }
}