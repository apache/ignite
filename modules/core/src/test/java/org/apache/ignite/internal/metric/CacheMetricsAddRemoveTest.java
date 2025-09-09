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

package org.apache.ignite.internal.metric;

import java.util.Arrays;
import com.google.common.collect.Iterators;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.spi.metric.HistogramMetric;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.cacheMetricsRegistryName;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.junit.Assert.assertArrayEquals;

/** */
@RunWith(Parameterized.class)
public class CacheMetricsAddRemoveTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE_GETS = "CacheGets";

    /** */
    public static final String CACHE_PUTS = "CachePuts";

    /** */
    public static final String GET_TIME = "GetTime";

    /** Test bounds. */
    public static final long[] BOUNDS = new long[] {50, 100};

    /** Cache modes. */
    @Parameterized.Parameters(name = "cacheMode={0},nearEnabled={1}")
    public static Iterable<Object[]> params() {
        return Arrays.asList(
            new Object[] {CacheMode.PARTITIONED, false},
            new Object[] {CacheMode.PARTITIONED, true},
            new Object[] {CacheMode.REPLICATED, false},
            new Object[] {CacheMode.REPLICATED, true}
        );
    }

    /** . */
    @Parameterized.Parameter(0)
    public CacheMode mode;

    /** Use index. */
    @Parameterized.Parameter(1)
    public boolean nearEnabled;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDataRegionConfigurations(
                    new DataRegionConfiguration().setName("persisted").setPersistenceEnabled(true)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();

        startGridsMultiThreaded(2);

        startClientGrid(getConfiguration("client"));
    }

    /** */
    @Test
    public void testCacheMetricsAddRemove() throws Exception {
        String cachePrefix = cacheMetricsRegistryName(DEFAULT_CACHE_NAME, false);

        checkMetricsEmpty(cachePrefix);

        createCache();

        checkMetricsNotEmpty(cachePrefix);

        destroyCache();

        checkMetricsEmpty(cachePrefix);
    }

    /** */
    @Test
    public void testCacheMetricsConfigurationNotRemovedOnStop() throws Exception {
        String cachePrefix = cacheMetricsRegistryName("other-cache", false);

        checkMetricsEmpty(cachePrefix);

        createCache("persisted", "other-cache", null);

        grid("client").cache("other-cache").put(1L, 1L);

        checkMetricsNotEmpty(cachePrefix);

        //Cache will be stopped during deactivation.
        grid("client").cluster().state(ClusterState.INACTIVE);

        checkMetricsEmpty(cachePrefix);

        grid("client").cluster().state(ClusterState.ACTIVE);

        assertEquals(1L, grid("client").cache("other-cache").get(1L));

        checkMetricsNotEmpty(cachePrefix);

        destroyCache();

        checkMetricsEmpty(cachePrefix);
    }

    /** */
    @Test
    public void testCacheMetricsConfigurationNotRemovedOnStopTwoCachesPerGroup() throws Exception {
        String cachePrefix = cacheMetricsRegistryName("other-cache", false);
        String cachePrefix2 = cacheMetricsRegistryName("other-cache2", false);

        checkMetricsEmpty(cachePrefix);
        checkMetricsEmpty(cachePrefix2);

        createCache("persisted", "other-cache", "testGroupName");
        createCache("persisted", "other-cache2", "testGroupName");

        grid("client").cache("other-cache").put(1L, 1L);
        grid("client").cache("other-cache2").put(1L, 1L);

        checkMetricsNotEmpty(cachePrefix);
        checkMetricsNotEmpty(cachePrefix2);

        //Cache will be stopped during deactivation.
        grid("client").cluster().state(ClusterState.INACTIVE);

        checkMetricsEmpty(cachePrefix);
        checkMetricsEmpty(cachePrefix2);

        grid("client").cluster().state(ClusterState.ACTIVE);

        assertEquals(1L, grid("client").cache("other-cache").get(1L));
        assertEquals(1L, grid("client").cache("other-cache2").get(1L));

        checkMetricsNotEmpty(cachePrefix);
        checkMetricsNotEmpty(cachePrefix2);

        destroyCache();

        checkMetricsEmpty(cachePrefix);
        checkMetricsEmpty(cachePrefix2);
    }

    /** */
    private void destroyCache() throws InterruptedException {
        IgniteEx client = grid("client");

        for (String name : client.cacheNames())
            client.destroyCache(name);

        awaitPartitionMapExchange();
    }

    /** */
    private void createCache() throws Exception {
        createCache(null, null, null);
    }

    /** */
    private void createCache(@Nullable String dataRegionName, @Nullable String cacheName,
        @Nullable String groupName) throws Exception {
        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        if (dataRegionName != null)
            ccfg.setDataRegionName(dataRegionName);

        if (cacheName != null)
            ccfg.setName(cacheName);

        if (groupName != null)
            ccfg.setGroupName(groupName);

        if (nearEnabled)
            ccfg.setNearConfiguration(new NearCacheConfiguration());

        grid("client").createCache(ccfg);

        awaitPartitionMapExchange();

        grid("client").context().metric().configureHistogram(
            MetricUtils.metricName(cacheMetricsRegistryName(ccfg.getName(), false), GET_TIME),
            BOUNDS);

        if (nearEnabled) {
            grid("client").context().metric().configureHistogram(
                MetricUtils.metricName(cacheMetricsRegistryName(ccfg.getName(), true), GET_TIME),
                BOUNDS);
        }
    }

    /** */
    private void checkMetricsNotEmpty(String cachePrefix) {
        for (int i = 0; i < 2; i++) {
            GridMetricManager mmgr = metricManager(i);

            MetricRegistry mreg = mmgr.registry(cachePrefix);

            assertNotNull(mreg.findMetric(CACHE_GETS));
            assertNotNull(mreg.findMetric(CACHE_PUTS));
            assertNotNull(mreg.findMetric(GET_TIME));
            assertArrayEquals(BOUNDS, mreg.<HistogramMetric>findMetric(GET_TIME).bounds());

            if (nearEnabled) {
                mreg = mmgr.registry(metricName(cachePrefix, "near"));

                assertNotNull(mreg.findMetric(CACHE_GETS));
                assertNotNull(mreg.findMetric(CACHE_PUTS));
                assertNotNull(mreg.findMetric(GET_TIME));
                assertArrayEquals(BOUNDS, mreg.<HistogramMetric>findMetric(GET_TIME).bounds());
            }
        }
    }

    /** */
    private void checkMetricsEmpty(String cachePrefix) {
        for (int i = 0; i < 3; i++) {
            GridMetricManager mmgr = metricManager(i);

            assertFalse(Iterators.tryFind(mmgr.iterator(), reg -> cachePrefix.equals(reg.name())).isPresent());

            if (nearEnabled) {
                String regName = metricName(cachePrefix, "near");

                assertFalse(Iterators.tryFind(mmgr.iterator(), reg -> regName.equals(reg.name())).isPresent());
            }
        }
    }

    /** */
    private GridMetricManager metricManager(int gridIdx) {
        if (gridIdx < 2)
            return grid(0).context().metric();
        else
            return grid("client").context().metric();
    }
}
