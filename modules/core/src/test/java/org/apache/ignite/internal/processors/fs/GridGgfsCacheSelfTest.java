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

package org.apache.ignite.internal.processors.fs;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.fs.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;

import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;

/**
 * Tests ensuring that GGFS data and meta caches are not "visible" through public API.
 */
public class GridGgfsCacheSelfTest extends GridGgfsCommonAbstractTest {
    /** Meta-information cache name. */
    private static final String META_CACHE_NAME = "meta";

    /** Data cache name. */
    private static final String DATA_CACHE_NAME = null;

    /** Regular cache name. */
    private static final String CACHE_NAME = "cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(cacheConfiguration(META_CACHE_NAME), cacheConfiguration(DATA_CACHE_NAME),
            cacheConfiguration(CACHE_NAME));

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);

        IgniteFsConfiguration ggfsCfg = new IgniteFsConfiguration();

        ggfsCfg.setMetaCacheName(META_CACHE_NAME);
        ggfsCfg.setDataCacheName(DATA_CACHE_NAME);
        ggfsCfg.setName("ggfs");

        cfg.setGgfsConfiguration(ggfsCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    protected CacheConfiguration cacheConfiguration(String cacheName) {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName(cacheName);

        if (META_CACHE_NAME.equals(cacheName))
            cacheCfg.setCacheMode(REPLICATED);
        else {
            cacheCfg.setCacheMode(PARTITIONED);
            cacheCfg.setDistributionMode(GridCacheDistributionMode.PARTITIONED_ONLY);

            cacheCfg.setBackups(0);
            cacheCfg.setAffinityMapper(new IgniteFsGroupDataBlocksKeyMapper(128));
        }

        cacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setQueryIndexEnabled(false);

        return cacheCfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * Test cache.
     *
     * @throws Exception If failed.
     */
    public void testCache() throws Exception {
        final Ignite g = grid();

        assert g.caches().size() == 1;

        assert CACHE_NAME.equals(g.caches().iterator().next().name());

        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Override public Object call() throws Exception {
                g.cache(META_CACHE_NAME);

                return null;
            }
        }, IllegalStateException.class, null);

        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Override public Object call() throws Exception {
                g.cache(DATA_CACHE_NAME);

                return null;
            }
        }, IllegalStateException.class, null);

        assert g.cache(CACHE_NAME) != null;
    }
}
