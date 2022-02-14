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

package org.apache.ignite.internal.processors.cache.warmup;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.warmup.LoadAllWarmUpStrategy.LoadPartition;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

/**
 * Test class for testing {@link LoadAllWarmUpStrategy}.
 */
public class LoadAllWarmUpStrategySelfTest extends GridCommonAbstractTest {
    /** Flag for enabling warm-up. */
    private boolean warmUp;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();

        LoadAllWarmUpStrategyEx.loadDataInfoCb = null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setPluginProviders(new WarmUpTestPluginProvider())
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDataRegionConfigurations(
                        new DataRegionConfiguration().setName("dr_0").setPersistenceEnabled(true)
                            .setWarmUpConfiguration(!warmUp ? null : new LoadAllWarmUpConfigurationEx()),
                        new DataRegionConfiguration().setName("dr_1").setPersistenceEnabled(true)
                            .setWarmUpConfiguration(!warmUp ? null : new LoadAllWarmUpConfigurationEx())
                    )
            ).setCacheConfiguration(
                cacheCfg("c_0", "g_0", "dr_0", Organization.queryEntity()),
                cacheCfg("c_1", "g_0", "dr_0", Person.queryEntity()),
                cacheCfg("c_2", "g_1", "dr_1", Organization.queryEntity()),
                cacheCfg("c_3", "g_1", "dr_1", Person.queryEntity())
            );
    }

    /**
     * Test checks that number of pages loaded is equal to number of pages warmed up.
     * <p/>
     * Steps:
     * 1)Start a node with static and dynamic caches and fill them in;
     * 2)Make a checkpoint and get number of pages loaded;
     * 3)Restart node and get number of pages warmed up;
     * 4)Check that number of loaded and warmed pages is equal;
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSimple() throws Exception {
        IgniteEx n = startGrid(0);
        n.cluster().state(ClusterState.ACTIVE);

        IgniteCache c4 = n.getOrCreateCache(cacheCfg("c_4", "g_2", "dr_0"));

        for (int i = 0; i < 5_000; i++) {
            n.cache("c_0").put("c_0" + i, new Organization(i, "c_0" + i));
            n.cache("c_1").put("c_1" + i, new Person(i, "c_1" + i, i));
            n.cache("c_2").put("c_2" + i, new Organization(i, "c_2" + i));
            n.cache("c_3").put("c_3" + i, new Person(i, "c_3" + i, i));

            c4.put("c_4" + i, ThreadLocalRandom.current().nextInt());
        }

        forceCheckpoint();

        Map<String, Long> expLoadedPages = loadedeDataRegionPages(n);

        stopAllGrids();

        warmUp = true;

        n = startGrid(0);

        Map<String, Long> actLoadedPages = loadedeDataRegionPages(n);

        assertEquals(expLoadedPages.size(), actLoadedPages.size());

        expLoadedPages.forEach((regName, loadedPages) -> {
            assertTrue(regName, actLoadedPages.containsKey(regName));
            assertEquals(regName, loadedPages, actLoadedPages.get(regName));
        });
    }

    /**
     * Test checks that if memory is less than pds, not all pages in pds will warm-up.
     * There may be evictions during warm-up, so count of pages loaded is not maximum.
     * <p/>
     * Steps:
     * 1)Start node and fill it with data for first data region until it is 2 * {@code MIN_PAGE_MEMORY_SIZE};
     * 2)Make a checkpoint;
     * 3)Restart node with warm-up, change maximum data region size to {@code MIN_PAGE_MEMORY_SIZE},
     * and listen for {@link LoadAllWarmUpStrategyEx#loadDataInfo};
     * 4)Check that estimated count of pages to warm-up is between maximum and
     * approximate minimum count of pages to load;
     * 5)Checking that total count of pages loaded is between maximum and
     * approximate minimum count of pages to load.
     *
     * Approximate value due to fact that there are already loaded pages at
     * beginning of warm-up, as well as evictions occur during warm-up.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMemoryLessPds() throws Exception {
        IgniteEx n = startGrid(0);
        n.cluster().state(ClusterState.ACTIVE);

        int i = 0;

        final long minMemSize = U.field(IgniteCacheDatabaseSharedManager.class, "MIN_PAGE_MEMORY_SIZE");

        DataRegion dr_0 = n.context().cache().context().database().dataRegion("dr_0");

        while (dr_0.pageMemory().loadedPages() * dr_0.pageMemory().systemPageSize() < 2 * minMemSize) {
            n.cache("c_0").put("c_0" + i, new Organization(i, "c_0" + i));
            n.cache("c_1").put("c_1" + i, new Person(i, "c_1" + i, i));

            i++;
        }

        forceCheckpoint();

        stopAllGrids();

        warmUp = true;

        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0));
        cfg.getDataStorageConfiguration().getDataRegionConfigurations()[0].setMaxSize(minMemSize);

        Map<String, Map<CacheGroupContext, List<LoadPartition>>> loadDataInfoMap = new ConcurrentHashMap<>();

        LoadAllWarmUpStrategyEx.loadDataInfoCb = loadDataInfoMap::put;

        n = startGrid(cfg);

        dr_0 = n.context().cache().context().database().dataRegion("dr_0");

        long warmUpPageCnt =
            loadDataInfoMap.get("dr_0").values().stream().flatMap(Collection::stream).mapToLong(LoadPartition::pages).sum();
        long maxLoadPages = minMemSize / dr_0.pageMemory().systemPageSize();
        long minLoadPages = maxLoadPages - 100;
        long loadPages = dr_0.pageMemory().loadedPages();

        // There are loaded pages before warm-up.
        assertTrue(warmUpPageCnt >= minLoadPages && warmUpPageCnt <= maxLoadPages);

        // Pages may be evicted.
        assertTrue(loadPages >= minLoadPages && loadPages <= maxLoadPages);
    }

    /**
     * Create cache configuration.
     *
     * @param name Cache name.
     * @param grpName Cache group name.
     * @param regName Data region name.
     * @param qryEntities Query entities.
     * @return New cache configuration.
     */
    private CacheConfiguration cacheCfg(String name, String grpName, String regName, QueryEntity... qryEntities) {
        requireNonNull(name);
        requireNonNull(grpName);
        requireNonNull(regName);

        return new CacheConfiguration(name)
            .setGroupName(grpName)
            .setDataRegionName(regName)
            .setAffinity(new GapRendezvousAffinityFunction(false, 5))
            .setQueryEntities(Arrays.asList(qryEntities));
    }

    /**
     * Counting of loaded pages for data regions.
     *
     * @param n Node.
     * @return Mapping: {dataRegionName -> loadedPageCnt}.
     */
    private Map<String, Long> loadedeDataRegionPages(IgniteEx n) {
        requireNonNull(n);

        return n.context().cache().cacheGroups().stream()
            .filter(grpCtx -> grpCtx.userCache() && grpCtx.persistenceEnabled())
            // Check for exists gap in local partitions.
            .peek(grpCtx -> assertTrue(grpCtx.topology().localPartitions().size() < grpCtx.topology().partitions()))
            .map(CacheGroupContext::dataRegion)
            .distinct()
            .collect(toMap(region -> region.config().getName(), region -> region.pageMemory().loadedPages()));
    }

    /**
     * {@link RendezvousAffinityFunction} for presence of a gap partition.
     */
    private static class GapRendezvousAffinityFunction extends RendezvousAffinityFunction {
        /** Gap partition id. */
        public static final int GAP_PART = 2;

        /**
         * Constructor that invoke {@link RendezvousAffinityFunction#RendezvousAffinityFunction(boolean, int)}.
         */
        public GapRendezvousAffinityFunction(boolean exclNeighbors, int parts) {
            super(exclNeighbors, parts);

            assert parts > GAP_PART : parts;
        }

        /** {@inheritDoc} */
        @Override public List<ClusterNode> assignPartition(
            int part,
            List<ClusterNode> nodes,
            int backups,
            @Nullable Map<UUID, Collection<ClusterNode>> neighborhoodCache
        ) {
            return part == GAP_PART ? emptyList() : super.assignPartition(part, nodes, backups, neighborhoodCache);
        }

        /** {@inheritDoc} */
        @Override public int partition(Object key) {
            int part = super.partition(key);

            return part == GAP_PART ? GAP_PART - 1 : part;
        }
    }
}
