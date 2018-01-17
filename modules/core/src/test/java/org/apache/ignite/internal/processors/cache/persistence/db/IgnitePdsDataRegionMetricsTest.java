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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.configuration.DataPageEvictionMode.RANDOM_2_LRU;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_DATA_REG_DEFAULT_NAME;

/** */
public class IgnitePdsDataRegionMetricsTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final long INIT_REGION_SIZE = 10 << 20;

    /** */
    private static final int ITERATIONS = 3;

    /** */
    private static final int BATCHES = 5;

    /** */
    private static final int BATCH_SIZE_LOW = 100;

    /** */
    private static final int BATCH_SIZE_HIGH = 1000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setInitialSize(INIT_REGION_SIZE)
                    .setPersistenceEnabled(true)
                    .setMetricsEnabled(true)
                    .setPageEvictionMode(RANDOM_2_LRU));

        cfg.setDataStorageConfiguration(memCfg);

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>()
            .setName(DEFAULT_CACHE_NAME)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(1);

        cfg.setCacheConfiguration(ccfg);

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

    /** {@inheritDoc} */
    protected long getTestTimeout() {
        return 15 * 60 * 1000;
    }

    /** */
    public void testMemoryUsageSingleNode() throws Exception {
        DataRegionMetrics initMetrics = null;

        for (int iter = 0; iter < ITERATIONS; iter++) {
            final IgniteEx node = startGrid(0);

            node.active(true);

            DataRegionMetrics currMetrics = getDfltRegionMetrics(node);

            if (initMetrics == null)
                initMetrics = currMetrics;

            assert currMetrics.getTotalAllocatedPages() >= currMetrics.getPhysicalMemoryPages();

            final IgniteCache<String, String> cache = node.getOrCreateCache(DEFAULT_CACHE_NAME);

            final Set<Integer> grpIds = getDfltRegGroupIds(node);

            Map<String, String> map = new HashMap<>();

            for (int batch = 0; batch < BATCHES; batch++) {
                int nPuts = BATCH_SIZE_LOW + ThreadLocalRandom.current().nextInt(BATCH_SIZE_HIGH - BATCH_SIZE_LOW);

                for (int i = 0; i < nPuts; i++)
                    map.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());

                cache.putAll(map);

                checkMetricsConsistency(node, grpIds);

                currMetrics = getDfltRegionMetrics(node);
            }

            assert currMetrics.getPhysicalMemoryPages() > initMetrics.getPhysicalMemoryPages();
            assert currMetrics.getTotalAllocatedPages() > initMetrics.getTotalAllocatedPages();

            node.close();
        }
    }

    /** */
    public void testMemoryUsageMultipleNodes() throws Exception {
        IgniteEx node0 = startGrid(0);

        IgniteEx node1 = startGrid(1);

        node0.active(true);

        final IgniteCache<String, String> cache = node0.getOrCreateCache(DEFAULT_CACHE_NAME);

        Map<String, String> map = new HashMap<>();

        int nPuts = BATCH_SIZE_LOW + ThreadLocalRandom.current().nextInt(BATCH_SIZE_HIGH - BATCH_SIZE_LOW);

        for (int i = 0; i < nPuts; i++)
            map.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        cache.putAll(map);

        awaitPartitionMapExchange(true, true, node0.cluster().nodes());

        node0.context().cache().context().database().waitForCheckpoint("");
        node1.context().cache().context().database().waitForCheckpoint("");

        checkMetricsConsistency(node0, getDfltRegGroupIds(node0));
        checkMetricsConsistency(node1, getDfltRegGroupIds(node1));

        IgniteEx node2 = startGrid(2);

        awaitPartitionMapExchange(true, true, node0.cluster().nodes());

        node0.context().cache().context().database().waitForCheckpoint("");
        node1.context().cache().context().database().waitForCheckpoint("");
        node2.context().cache().context().database().waitForCheckpoint("");

        checkMetricsConsistency(node0, getDfltRegGroupIds(node0));
        checkMetricsConsistency(node1, getDfltRegGroupIds(node1));
        checkMetricsConsistency(node2, getDfltRegGroupIds(node2));

        stopGrid(1);

        awaitPartitionMapExchange(true, true, node0.cluster().nodes());

        node0.context().cache().context().database().waitForCheckpoint("");
        node2.context().cache().context().database().waitForCheckpoint("");

        checkMetricsConsistency(node0, getDfltRegGroupIds(node0));
        checkMetricsConsistency(node2, getDfltRegGroupIds(node2));
    }

    /** */
    private static DataRegionMetrics getDfltRegionMetrics(Ignite node) {
        for (DataRegionMetrics m : node.dataRegionMetrics())
            if (DFLT_DATA_REG_DEFAULT_NAME.equals(m.getName()))
                return m;

        throw new RuntimeException("No metrics found for default data region");
    }

    /** */
    private static Set<Integer> getDfltRegGroupIds(IgniteEx node) {
        Set<Integer> grpIds = new HashSet<>();

        for (Object ctx : node.context().cache().context().cacheContexts()) {
            CacheGroupContext grp = ((GridCacheContext)ctx).group();
            if (DFLT_DATA_REG_DEFAULT_NAME.equals(grp.dataRegion().config().getName()))
                grpIds.add(grp.groupId());
        }

        return grpIds;
    }

    /** */
    private static void checkMetricsConsistency (
        final IgniteEx node,
        final Set<Integer> grpIds
    ) throws Exception
    {
        boolean storageMatches = GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                long pagesInStore = 0;

                for (int grpId : grpIds)
                    pagesInStore += node.context().cache().context().pageStore().pagesAllocated(grpId);

                long allocated = getDfltRegionMetrics(node).getTotalAllocatedPages();

                node.log().error(">>>>>>> " + node.name() + "  " + allocated + " " + pagesInStore);

                return allocated == pagesInStore;
            }
        }, 1000);

        assert storageMatches;

        DataRegionMetrics m = getDfltRegionMetrics(node);

       // assert m.getTotalAllocatedPages() >= m.getPhysicalMemoryPages();
    }
}
