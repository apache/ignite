/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
@RunWith(JUnit4.class)
public class GridCacheNearTxForceKeyTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setRebalanceMode(ASYNC);
        ccfg.setRebalanceDelay(5000);
        ccfg.setBackups(0);
        ccfg.setNearConfiguration(new NearCacheConfiguration());

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * Test provokes scenario when primary node sends force key request to node started transaction.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNearTx() throws Exception {
        Ignite ignite0 = startGrid(0);

        IgniteCache<Integer, Integer> cache = ignite0.cache(DEFAULT_CACHE_NAME);

        Ignite ignite1 = startGrid(1);

        awaitPartitionMapExchange();

        // This key should become primary for ignite1.
        final Integer key = primaryKey(ignite1.cache(DEFAULT_CACHE_NAME));

        assertNull(cache.getAndPut(key, key));

        assertTrue(ignite0.affinity(DEFAULT_CACHE_NAME).isPrimary(ignite1.cluster().localNode(), key));
    }
}
