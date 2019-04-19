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

package org.apache.ignite.loadtests;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Multi-node cache test.
 */
public class GridCacheMultiNodeLoadTest extends GridCommonAbstractTest {
    /** Cache name. */
    public static final String CACHE_NAME = "partitioned";

    /** Elements count. */
    public static final int ELEMENTS_COUNT = 200000;

    /** Grid 1. */
    private static Ignite ignite1;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName(CACHE_NAME);
        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setNearConfiguration(null);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);

        LruEvictionPolicy plc = new LruEvictionPolicy();
        plc.setMaxSize(100000);

        cacheCfg.setEvictionPolicy(plc);
        cacheCfg.setOnheapCacheEnabled(true);
        cacheCfg.setBackups(1);

        cacheCfg.setRebalanceMode(SYNC);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        ignite1 = startGrid(1);
        startGrid(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        ignite1 = null;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return Long.MAX_VALUE;
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testMany() throws Exception {
        ignite1.compute().execute(GridCacheLoadPopulationTask.class, null);
    }
}
