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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests cache deploy on topology from static configuration.
 */
@RunWith(JUnit4.class)
public class IgniteStaticCacheStartSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "TestCache";

    /** */
    private boolean hasCache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (hasCache) {
            CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

            ccfg.setCacheMode(CacheMode.PARTITIONED);
            ccfg.setBackups(1);
            ccfg.setName(CACHE_NAME);
            ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
            ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);

            cfg.setCacheConfiguration(ccfg);
        }

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeployCacheOnNodeStart() throws Exception {
        startGrids(3);

        try {
            hasCache = true;

            startGrid(3);

            for (int i = 0; i < 4; i++) {
                info("Checking ignite: " + i);

                Ignite ignite = ignite(i);

                IgniteCache<Object, Object> jcache = ignite.cache(CACHE_NAME);

                assertNotNull(jcache);

                jcache.put(i, i);
            }

            hasCache = false;

            startGrid(4);

            for (int i = 0; i < 5; i++) {
                info("Checking ignite: " + i);

                Ignite ignite = ignite(i);

                IgniteCache<Object, Object> jcache = ignite.cache(CACHE_NAME);

                assertNotNull(jcache);

                if (i != 4)
                    assertEquals(i, jcache.get(i));
            }
        }
        finally {
            stopAllGrids();
        }
    }
}
