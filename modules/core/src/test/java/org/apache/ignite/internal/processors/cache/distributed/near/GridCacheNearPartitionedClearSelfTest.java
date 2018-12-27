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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheGenericTestStore;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Test clear operation in NEAR_PARTITIONED transactional cache.
 */
@SuppressWarnings("unchecked")
@RunWith(JUnit4.class)
public class GridCacheNearPartitionedClearSelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 3;

    /** Backup count. */
    private static final int BACKUP_CNT = 1;

    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** */
    private static CacheStore<Object, Object> store = new GridCacheGenericTestStore<>();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        MvccFeatureChecker.failIfNotSupported(MvccFeatureChecker.Feature.NEAR_CACHE);

        startGrids(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        G.stopAll(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setLocalHost("127.0.0.1");

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setName(CACHE_NAME);
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setNearConfiguration(new NearCacheConfiguration());
        ccfg.setRebalanceMode(SYNC);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setBackups(BACKUP_CNT);
        ccfg.setCacheStoreFactory(singletonFactory(store));
        ccfg.setReadThrough(true);
        ccfg.setWriteThrough(true);
        ccfg.setLoadPreviousValue(true);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * Test clear.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClear() throws Exception {
        IgniteCache cache = cacheForIndex(0);

        int key = primaryKey0(grid(0), cache);

        cache.put(key, 1);
        cache.clear();

        for (int i = 0; i < GRID_CNT; i++) {
            IgniteCache cache0 = cacheForIndex(i);

            cache0.removeAll();

            assert cache0.localSize() == 0;
        }

        cache.put(key, 1);
        cache.clear();

        assertEquals(0, cache.size());
    }

    /**
     * Gets primary key for the given cache.
     *
     * @param cache Cache.
     * @return Primary key.
     * @throws Exception If failed.
     */
    private int primaryKey0(Ignite ignite, IgniteCache cache) throws Exception {
        ClusterNode locNode = ignite.cluster().localNode();

        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            if (affinity(cache).isPrimary(locNode, i))
                return i;
        }

        throw new Exception("Cannot determine affinity key.");
    }

    /**
     * Gets cache for the node with the given index.
     *
     * @param idx Index.
     * @return Cache.
     */
    private IgniteCache cacheForIndex(int idx) {
        return grid(idx).cache(CACHE_NAME);
    }
}
