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

package org.apache.ignite.internal.processors.cache.persistence.db;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.configuration.DataPageEvictionMode.RANDOM_2_LRU;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_DATA_REG_DEFAULT_NAME;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE;

/** */
public class IgnitePdsDataRegionMetricsTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final long INIT_REGION_MAX_SIZE = 10 << 20;

    /** */
    private static final long DFLT_REGION_MAX_SIZE = 40 << 20;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setInitialSize(INIT_REGION_MAX_SIZE)
                    .setMaxSize(DFLT_REGION_MAX_SIZE)
                    .setPersistenceEnabled(true)
                    .setMetricsEnabled(true)
                    .setPageEvictionMode(RANDOM_2_LRU));

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        GridTestUtils.deleteDbFiles();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        GridTestUtils.deleteDbFiles();

        super.afterTest();
    }

    /** */
    public void testMemoryUsageGrowth() throws Exception {
        Ignite node = startGrid(0);

        node.active(true);

        DataRegionMetrics m = getDfltRegionMetrics();

        assert m.getTotalAllocatedPages() >= m.getPhysicalMemoryPages();

        IgniteCache<String,String> cache = node.getOrCreateCache(DEFAULT_CACHE_NAME);

        Map<String, String> map = new HashMap<>();

        while (m.getTotalAllocatedPages() * DFLT_PAGE_SIZE < 0.9 * DFLT_REGION_MAX_SIZE) {
            for (int i = 0; i < 500; i++)
                map.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());

            cache.putAll(map);

            DataRegionMetrics mNext = getDfltRegionMetrics();

            assert mNext.getTotalAllocatedPages() > m.getTotalAllocatedPages();
            assert mNext.getPhysicalMemoryPages() > m.getPhysicalMemoryPages();
            assert mNext.getTotalAllocatedPages() >= mNext.getPhysicalMemoryPages();

            m = mNext;

            map.clear();
        }

        long ts = System.currentTimeMillis();

        do {
            for (int i = 0; i < 500; i++)
                cache.get(UUID.randomUUID().toString());
        } while (System.currentTimeMillis() - ts < 5000);

        DataRegionMetrics mLast = getDfltRegionMetrics();

        assert mLast.getTotalAllocatedPages() == m.getTotalAllocatedPages();
        assert mLast.getPhysicalMemoryPages() == m.getPhysicalMemoryPages();
    }

    /** */
    private DataRegionMetrics getDfltRegionMetrics() {
        for (DataRegionMetrics m : grid(0).dataRegionMetrics())
            if (DFLT_DATA_REG_DEFAULT_NAME.equals(m.getName()))
                return m;

        throw new AssertionError("No metrics found for default data region");
    }
}
