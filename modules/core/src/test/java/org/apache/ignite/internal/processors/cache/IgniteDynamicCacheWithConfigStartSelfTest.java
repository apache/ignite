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

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class IgniteDynamicCacheWithConfigStartSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "partitioned";

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (client)
            cfg.setCacheConfiguration(cacheConfiguration());

        cfg.setClientMode(client);

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setIndexedTypes(String.class, String.class);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartCacheOnClient() throws Exception {
        int srvCnt = 3;

        startGrids(srvCnt);

        try {
            client = true;

            IgniteEx client = startGrid(srvCnt);

            for (int i = 0; i < 100; i++)
                client.cache(CACHE_NAME).put(i, i);

            for (int i = 0; i < 100; i++)
                assertEquals(i, grid(0).cache(CACHE_NAME).get(i));

            client.cache(CACHE_NAME).removeAll();

            for (int i = 0; i < 100; i++)
                assertNull(grid(0).cache(CACHE_NAME).get(i));
        }
        finally {
            stopAllGrids();
        }
    }
}
