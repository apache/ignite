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

package org.apache.ignite.internal.processors.cache;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.rendezvous.ClusterNodeAttributeAffinityBackupFilter;
import org.apache.ignite.cache.affinity.rendezvous.ClusterNodeAttributeColocatedBackupFilter;
import org.apache.ignite.cache.affinity.rendezvous.MdcAffinityBackupFilter;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.spi.metric.BooleanMetric;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_DATA_CENTER_ID;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.cacheMetricsRegistryName;

/**
 * Test for new cache metrics for highlighting two data safety issues in Multi DataCenter environments:
 * 1. If cache configuration doesn't specify an affinity backup filter that could guarantee presence of data copy in each DC.
 * 2. If cluster topology changed in such a way that partition copies are not spread across all available DCs.
 */
public class MdcCacheMetricsTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES_NUMBER = 5;

    /** */
    private static final String CACHE_WITH_MDC_FILTER = "mdcSafeCache0";

    /** */
    private static final String CACHE_WITH_COLOCATED_FILTER = "mdcSafeCache1";

    /** */
    private static final String CACHE_WITH_MDC_SAFE_ATTRIBUTE_FILTER = "mdcSafeCache2";

    /** */
    private static final String MDC_UNSAFE_CACHE = "mdcUnsafeCache0";

    /** */
    private static final String CACHE_WITH_MDC_UNSAFE_ATTRIBUTE_FILTER = "mdcUnsafeCache1";

    /** */
    private static final String STRETCHED_CELL_ATTR_NAME = "DC_CELL_ATTR";

    /** */
    private static final String ATTR_FOR_UNSAFE_ATTR_FILTER = "MDC_UNAWARE_ATTR" ;

    /** */
    private static final String[] STRETCHED_CELL_IDS = {"CELL_0", "CELL_1"};

    /** */
    private static final String DC_ID_0 = "DC_0";

    /** */
    private static final String DC_ID_1 = "DC_1";

    /** */
    private static final String AFFINITY_CFG_MDC_SAFE_METRIC_NAME = "IsCacheAffinityConfigurationMdcSafe";

    /** */
    private static final String PARTITION_DISTRIBUTION_SAFE_METRIC_NAME = "IsCachePartitionDistributionSafe";

    /** */
    private String dcId;

    /** */
    private String cellId;

    /** */
    private boolean useStaticCaches;

    /** */
    private boolean persistenceEnabled;

    /** */
    private Set<String> allCaches = new HashSet<>();

    /** */
    private Set<String> mdcSafeCaches = new HashSet<>();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        System.clearProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID);

        allCaches.clear();
        mdcSafeCaches.clear();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(persistenceEnabled)
                .setMaxSize(32 * 1024 * 1024)
            ));

        if (useStaticCaches) {
            CacheConfiguration mdcSafeCacheCfg0 = prepareCacheCfg(
                CACHE_WITH_MDC_FILTER,
                new MdcAffinityBackupFilter(2, 1),
                true);

            CacheConfiguration mdcSafeCacheCfg1 = prepareCacheCfg(
                CACHE_WITH_COLOCATED_FILTER,
                new ClusterNodeAttributeColocatedBackupFilter(STRETCHED_CELL_ATTR_NAME),
                true);

            CacheConfiguration mdcUnsafeCacheCfg0 = prepareCacheCfg(MDC_UNSAFE_CACHE, null, false);

            CacheConfiguration mdcUnsafeCacheCfg1 = prepareCacheCfg(
                CACHE_WITH_MDC_UNSAFE_ATTRIBUTE_FILTER,
                new ClusterNodeAttributeAffinityBackupFilter(ATTR_FOR_UNSAFE_ATTR_FILTER),
                false);

            cfg.setCacheConfiguration(mdcSafeCacheCfg0, mdcSafeCacheCfg1, mdcUnsafeCacheCfg0, mdcUnsafeCacheCfg1);
        }

        if (!cfg.isClientMode())
            cfg.setUserAttributes(F.asMap(
                STRETCHED_CELL_ATTR_NAME,
                cellId,
                IgniteSystemProperties.IGNITE_DATA_CENTER_ID,
                dcId));

        return cfg;
    }

    /** */
    private CacheConfiguration prepareCacheCfg(
        String cacheName,
        IgniteBiPredicate<ClusterNode, List<ClusterNode>> affBackupFilter,
        boolean affCfgMdcSafe) {
        CacheConfiguration cacheCfg = new CacheConfiguration(cacheName)
            .setCacheMode(PARTITIONED)
            .setBackups(1);

        cacheCfg.setAffinity(
            new RendezvousAffinityFunction()
                .setPartitions(32)
                .setAffinityBackupFilter(affBackupFilter));

        if (affCfgMdcSafe)
            mdcSafeCaches.add(cacheName);

        allCaches.add(cacheName);

        return cacheCfg;
    }

    /** */
    @Test
    public void testAffinityCfgMdcSafeMetricForDynamicCaches() throws Exception {
        useStaticCaches = false;

        startClusterAcrossDataCenters(new String[] {DC_ID_0, DC_ID_1}, 2);

        IgniteEx client = startClientGrid(NODES_NUMBER - 1);

        client.cluster().state(ClusterState.ACTIVE);

        client.getOrCreateCache(
            prepareCacheCfg(CACHE_WITH_MDC_FILTER, new MdcAffinityBackupFilter(2, 1), true));

        client.getOrCreateCache(
            prepareCacheCfg(CACHE_WITH_COLOCATED_FILTER, new ClusterNodeAttributeColocatedBackupFilter(STRETCHED_CELL_ATTR_NAME), true));

        client.getOrCreateCache(
            prepareCacheCfg(MDC_UNSAFE_CACHE, null, false));

        client.getOrCreateCache(
            prepareCacheCfg(CACHE_WITH_MDC_UNSAFE_ATTRIBUTE_FILTER,
                new ClusterNodeAttributeAffinityBackupFilter(ATTR_FOR_UNSAFE_ATTR_FILTER), false));

        checkMdcReadyMetric();
    }

    /** */
    @Test
    public void testAffinityCfgMdcSafeMetricForStaticCaches() throws Exception {
        useStaticCaches = true;

        startClusterAcrossDataCenters(new String[] {DC_ID_0, DC_ID_1}, 2);

        IgniteEx client = startClientGrid(NODES_NUMBER - 1);

        client.cluster().state(ClusterState.ACTIVE);

        checkMdcReadyMetric();
    }

    /** */
    @Test
    public void testPartitionDistributionMetricInMemoryCaches() throws Exception {
        persistenceEnabled = false;

        startClusterAcrossDataCenters(new String[] {DC_ID_0, DC_ID_1}, 2);

        IgniteEx client = startClientGrid(NODES_NUMBER - 1);

        client.getOrCreateCache(prepareCacheCfg(CACHE_WITH_MDC_FILTER, new MdcAffinityBackupFilter(2, 1), true));
        client.getOrCreateCache(prepareCacheCfg(CACHE_WITH_COLOCATED_FILTER,
            new ClusterNodeAttributeColocatedBackupFilter(STRETCHED_CELL_ATTR_NAME), true));
        client.getOrCreateCache(prepareCacheCfg(CACHE_WITH_MDC_SAFE_ATTRIBUTE_FILTER,
            new ClusterNodeAttributeAffinityBackupFilter(ATTR_DATA_CENTER_ID), true));

        BooleanMetric cacheWithMdcFilterDistributionSafeMetric = findMetricForCache(
            grid(1),
            CACHE_WITH_MDC_FILTER,
            PARTITION_DISTRIBUTION_SAFE_METRIC_NAME);
        BooleanMetric cacheWithColocatedFilterDistributionSafeMetric = findMetricForCache(
            grid(1),
            CACHE_WITH_COLOCATED_FILTER,
            PARTITION_DISTRIBUTION_SAFE_METRIC_NAME);
        BooleanMetric cacheWithMdcSafeAttrFilterDistributionSafeMetric = findMetricForCache(
            grid(1),
            CACHE_WITH_MDC_SAFE_ATTRIBUTE_FILTER,
            PARTITION_DISTRIBUTION_SAFE_METRIC_NAME
        );

        assertNotNull(cacheWithMdcFilterDistributionSafeMetric);
        assertNotNull(cacheWithColocatedFilterDistributionSafeMetric);
        assertNotNull(cacheWithColocatedFilterDistributionSafeMetric);
        assertTrue(cacheWithMdcFilterDistributionSafeMetric.value());
        assertTrue(cacheWithColocatedFilterDistributionSafeMetric.value());
        assertTrue(cacheWithMdcSafeAttrFilterDistributionSafeMetric.value());

        stopGrid(0);

        assertTrue(cacheWithMdcFilterDistributionSafeMetric.value());
        assertTrue(cacheWithMdcSafeAttrFilterDistributionSafeMetric.value());
        assertFalse(cacheWithColocatedFilterDistributionSafeMetric.value());
    }

    /** */
    @Test
    public void testPartitionDistributionMetricPersistentCaches() throws Exception {
        persistenceEnabled = true;

        startClusterAcrossDataCenters(new String[] {DC_ID_0, DC_ID_1}, 2);

        IgniteEx client = startClientGrid(NODES_NUMBER - 1);

        client.cluster().state(ClusterState.ACTIVE);

        client.getOrCreateCache(prepareCacheCfg(CACHE_WITH_MDC_FILTER, new MdcAffinityBackupFilter(2, 1), true));
        client.getOrCreateCache(prepareCacheCfg(CACHE_WITH_COLOCATED_FILTER,
            new ClusterNodeAttributeColocatedBackupFilter(STRETCHED_CELL_ATTR_NAME), true));
        client.getOrCreateCache(prepareCacheCfg(CACHE_WITH_MDC_SAFE_ATTRIBUTE_FILTER,
            new ClusterNodeAttributeAffinityBackupFilter(ATTR_DATA_CENTER_ID), true));

        BooleanMetric cacheWithMdcFilterDistributionSafeMetric = findMetricForCache(
            grid(1),
            CACHE_WITH_MDC_FILTER,
            PARTITION_DISTRIBUTION_SAFE_METRIC_NAME);
        BooleanMetric cacheWithColocatedFilterDistributionSafeMetric = findMetricForCache(
            grid(1),
            CACHE_WITH_COLOCATED_FILTER,
            PARTITION_DISTRIBUTION_SAFE_METRIC_NAME);
        BooleanMetric cacheWithMdcSafeAttrFilterDistributionSafeMetric = findMetricForCache(
            grid(1),
            CACHE_WITH_MDC_SAFE_ATTRIBUTE_FILTER,
            PARTITION_DISTRIBUTION_SAFE_METRIC_NAME
        );

        assertNotNull(cacheWithMdcFilterDistributionSafeMetric);
        assertNotNull(cacheWithColocatedFilterDistributionSafeMetric);
        assertNotNull(cacheWithMdcSafeAttrFilterDistributionSafeMetric);
        assertTrue(cacheWithMdcFilterDistributionSafeMetric.value());
        assertTrue(cacheWithColocatedFilterDistributionSafeMetric.value());
        assertTrue(cacheWithMdcSafeAttrFilterDistributionSafeMetric.value());

        stopGrid(0);

        assertFalse(cacheWithMdcFilterDistributionSafeMetric.value());
        assertFalse(cacheWithColocatedFilterDistributionSafeMetric.value());
        assertFalse(cacheWithMdcSafeAttrFilterDistributionSafeMetric.value());

        client.cluster().setBaselineTopology(client.cluster().topologyVersion());

        // MdcAffinityBackupFilter and ClusterNodeAttributeAffinityBackupFilter are able to reassing partitions
        // after BaselineTopology change, thus distribution safe metric restores to true.
        assertTrue(cacheWithMdcFilterDistributionSafeMetric.value());
        assertTrue(cacheWithMdcSafeAttrFilterDistributionSafeMetric.value());
        // But ClusterNodeAttributeColocatedBackupFilter doesn't reassing partitions after BaselineTopology change
        // so distribution safe metric for this cache remains false.
        assertFalse(cacheWithColocatedFilterDistributionSafeMetric.value());

    }

    /** */
    private BooleanMetric findMetricForCache(IgniteEx grid, String cacheName, String metricName) {
        GridCacheContext<Object, Object> cacheCtx = grid.cachex(cacheName).context();

        return cacheCtx.kernalContext().metric().registry(cacheMetricsRegistryName(
            cacheCtx.name(), cacheCtx.cache().isNear())).findMetric(metricName);
    }

    /** */
    private void checkMdcReadyMetric() {
        for (int i = 0; i < NODES_NUMBER; i++) {
            IgniteEx ig = grid(i);

            for (String cacheName : allCaches) {
                BooleanMetric cacheMdcSafeMetric = findMetricForCache(ig, cacheName, AFFINITY_CFG_MDC_SAFE_METRIC_NAME);

                if (ig.localNode().isClient()) {
                    assertNull(cacheMdcSafeMetric);

                    continue;
                }
                else
                    assertNotNull("Grid: " + i + ", cache: " + cacheName, cacheMdcSafeMetric);

                boolean cacheMdcSafe = cacheMdcSafeMetric.value();

                assertEquals(mdcSafeCaches.contains(cacheName), cacheMdcSafe);
            }
        }
    }

    /** */
    private IgniteEx startClusterAcrossDataCenters(String[] dcIds, int nodesPerDc) throws Exception {
        int nodeIdx = 0;
        IgniteEx lastNode = null;

        for (String dcId : dcIds) {
            this.dcId = dcId;

            for (int i = 0; i < nodesPerDc; i++) {
                cellId = STRETCHED_CELL_IDS[i];

                lastNode = startGrid(nodeIdx++);
            }
        }

        return lastNode;
    }
}
