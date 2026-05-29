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

package org.apache.ignite.internal.processors.cache.metric;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteSystemProperties;
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
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.spi.metric.BooleanMetric;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.cacheMetricsRegistryName;

/** */
public class MdcCacheMetricsTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES_NUMBER = 5;
    /** */
    private static final String MDC_SAFE_CACHE_0 = "mdcSafeCache0";
    /** */
    private static final String MDC_SAFE_CACHE_1 = "mdcSafeCache1";
    /** */
    private static final String MDC_UNSAFE_CACHE = "mdcUnsafeCache";
    /** */
    private static final String STRETCHED_CELL_ATTR_NAME = "DC_CELL_ATTR";
    /** */
    private static final String DC_ID_0 = "DC_0";
    /** */
    private static final String DC_ID_1 = "DC_1";
    /** */
    private String curNodeDcId;
    /** */
    private boolean useStaticCaches;
    /** */
    private Set<String> allCaches = new HashSet<>();
    /** */
    private Set<String> mdcSafeCaches = new HashSet<>();
    /** */
    private Set<String> mdcUnsafeCaches = new HashSet<>();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        System.clearProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID);

        allCaches.clear();
        mdcSafeCaches.clear();
        mdcUnsafeCaches.clear();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();

        System.clearProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(32 * 1024 * 1024)
            ));

        if (useStaticCaches) {
            CacheConfiguration mdcSafeCacheCfg0 = prepareCacheCfg(MDC_SAFE_CACHE_0, new MdcAffinityBackupFilter(2, 1));

            CacheConfiguration mdcSafeCacheCfg1 = prepareCacheCfg(
                MDC_SAFE_CACHE_1,
                new ClusterNodeAttributeColocatedBackupFilter(STRETCHED_CELL_ATTR_NAME));

            CacheConfiguration mdcUnsafeCacheCfg = prepareCacheCfg(MDC_UNSAFE_CACHE, null);

            cfg.setCacheConfiguration(mdcSafeCacheCfg0, mdcSafeCacheCfg1, mdcUnsafeCacheCfg);
        }

        if (!cfg.isClientMode())
            cfg.setUserAttributes(F.asMap(STRETCHED_CELL_ATTR_NAME, curNodeDcId));

        return cfg;
    }

    /** */
    private CacheConfiguration prepareCacheCfg(
        String cacheName,
        IgniteBiPredicate<ClusterNode, List<ClusterNode>> affinityBackupFilter)
    {
        CacheConfiguration cacheCfg = new CacheConfiguration(cacheName)
            .setCacheMode(PARTITIONED)
            .setBackups(1);

        if (affinityBackupFilter != null) {
            cacheCfg.setAffinity(
                new RendezvousAffinityFunction()
                    .setPartitions(32)
                    .setAffinityBackupFilter(affinityBackupFilter));

            mdcSafeCaches.add(cacheName);
        }

        allCaches.add(cacheName);

        return cacheCfg;
    }

    /** */
    @Test
    public void testMdcAffinityReadyMetricForDynamicCaches() throws Exception {
        useStaticCaches = false;

        startClusterAcrossDataCenters(new String[] {DC_ID_0, DC_ID_1}, 2);

        IgniteEx client = startClientGrid(NODES_NUMBER - 1);

        client.cluster().state(ClusterState.ACTIVE);

        client.getOrCreateCache(prepareCacheCfg(MDC_SAFE_CACHE_0, new MdcAffinityBackupFilter(2, 1)));

        client.getOrCreateCache(prepareCacheCfg(MDC_SAFE_CACHE_1, new ClusterNodeAttributeColocatedBackupFilter(STRETCHED_CELL_ATTR_NAME)));

        client.getOrCreateCache(prepareCacheCfg(MDC_UNSAFE_CACHE, null));

        checkMdcReadyMetric();
    }

    /** */
    @Test
    public void testMdcAffinityReadyMetricForStaticCaches() throws Exception {
        useStaticCaches = true;

        startClusterAcrossDataCenters(new String[] {DC_ID_0, DC_ID_1}, 2);

        IgniteEx client = startClientGrid(NODES_NUMBER - 1);

        client.cluster().state(ClusterState.ACTIVE);

        checkMdcReadyMetric();
    }

    @Test
    public void testExchangeResearch() throws Exception {
        useStaticCaches = false;

        startClusterAcrossDataCenters(new String[] {DC_ID_0, DC_ID_1}, 1);

        IgniteEx client = startClientGrid(NODES_NUMBER - 1);

        client.cluster().state(ClusterState.ACTIVE);

        client.getOrCreateCache(prepareCacheCfg(MDC_SAFE_CACHE_0, new MdcAffinityBackupFilter(2, 1)));

        stopGrid(0);

        Thread.sleep(1000);
    }

    /** */
    private void checkMdcReadyMetric() {
        for (int i = 0; i < NODES_NUMBER; i++) {
            IgniteEx ig = grid(i);

            for (String cacheName : allCaches) {
                GridCacheContext<Object, Object> cctx = ig.cachex(cacheName).context();

                MetricRegistryImpl mReg = cctx.kernalContext().metric().registry(cacheMetricsRegistryName(cctx.name(), cctx.cache().isNear()));

                BooleanMetric cacheMdcSafeMetric = mReg.findMetric("IsCacheAffinityMdcReady");

                System.out.println("-->>-->> [" + System.currentTimeMillis() + "][" + Thread.currentThread().getName() + "] checking cache: " + cacheName);

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
            System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, dcId);

            curNodeDcId = dcId;

            for (int i = 0; i < nodesPerDc; i++)
                lastNode = startGrid(nodeIdx++);
        }

        System.clearProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID);

        return lastNode;
    }
}
