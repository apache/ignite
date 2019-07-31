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
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class CacheConfigurationLeakTest extends GridCommonAbstractTest {
    /**
     *
     */
    public CacheConfigurationLeakTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        DataStorageConfiguration memCfg = new DataStorageConfiguration();

        DataRegionConfiguration plc = new DataRegionConfiguration();

        plc.setName("dfltPlc");
        plc.setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_MAX_SIZE * 10);

        memCfg.setDefaultDataRegionConfiguration(plc);

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheCreateLeak() throws Exception {
        final Ignite ignite = grid();

        GridTestUtils.runMultiThreaded(idx -> {
            for (int i = 0; i < GridTestUtils.SF.applyLB(100, 50); i++) {
                CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
                ccfg.setName("cache-" + idx + "-" + i);
                ccfg.setEvictionPolicy(new LruEvictionPolicy(1000));
                ccfg.setOnheapCacheEnabled(true);

                IgniteCache<Object, Object> cache = ignite.createCache(ccfg);

                for (int k = 0; k < GridTestUtils.SF.applyLB(5000, 3000); k++)
                    cache.put(k, new byte[1024]);

                ignite.destroyCache(cache.getName());
            }
        }, 5, "cache-thread");
    }
}
