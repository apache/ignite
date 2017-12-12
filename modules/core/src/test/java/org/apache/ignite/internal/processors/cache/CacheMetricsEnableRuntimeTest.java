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

import java.lang.management.ManagementFactory;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.CacheMetricsMXBean;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class CacheMetricsEnableRuntimeTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String CACHE1 = "cache1";

    /** */
    private static final String CACHE2 = "cache2";

    /** */
    private static final String GROUP = "group1";

    /** Wait condition timeout. */
    private static final long WAIT_CONDITION_TIMEOUT = 3_000L;

    /** Persistence. */
    private boolean persistence = false;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Gets CacheGroupMetricsMXBean for given node and group name.
     *
     * @param nodeIdx Node index.
     * @param cacheName Cache name.
     * @return MBean instance.
     */
    private CacheMetricsMXBean mxBean(int nodeIdx, String cacheName, Class<? extends CacheMetricsMXBean> clazz)
        throws MalformedObjectNameException {
        ObjectName mbeanName = U.makeCacheMBeanName(getTestIgniteInstanceName(nodeIdx), cacheName,
            clazz.getName());

        MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();

        if (!mbeanSrv.isRegistered(mbeanName))
            fail("MBean is not registered: " + mbeanName.getCanonicalName());

        return MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName, CacheMetricsMXBean.class,
            true);
    }


    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        CacheConfiguration cacheCfg = new CacheConfiguration()
            .setName(CACHE1)
            .setGroupName(GROUP)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC);

        cfg.setCacheConfiguration(cacheCfg);

        if (persistence)
            cfg.setDataStorageConfiguration(new DataStorageConfiguration().setWalMode(WALMode.LOG_ONLY));

        return cfg;
    }

    /**
     * @param persistence Persistence.
     */
    private void testJmxStatisticsEnable(boolean persistence) throws Exception {
        this.persistence = persistence;

        final Ignite ig1 = startGrid(1);
        final Ignite ig2 = startGrid(2);

        CacheConfiguration cacheCfg2 = new CacheConfiguration(ig1.cache(CACHE1).getConfiguration(
            CacheConfiguration.class));

        cacheCfg2.setName(CACHE2);
        cacheCfg2.setStatisticsEnabled(true);

        ig2.getOrCreateCache(cacheCfg2);

        CacheMetricsMXBean mxBeanCache1 = mxBean(2, CACHE1, CacheClusterMetricsMXBeanImpl.class);
        CacheMetricsMXBean mxBeanCache2 = mxBean(2, CACHE2, CacheClusterMetricsMXBeanImpl.class);
        CacheMetricsMXBean mxBeanCache1loc = mxBean(2, CACHE1, CacheLocalMetricsMXBeanImpl.class);

        mxBeanCache1.enableStatistics();
        mxBeanCache2.disableStatistics();

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return ig1.cache(CACHE1).metrics().isStatisticsEnabled()
                    && ig2.cache(CACHE1).metrics().isStatisticsEnabled()
                    && !ig1.cache(CACHE2).metrics().isStatisticsEnabled()
                    && !ig2.cache(CACHE2).metrics().isStatisticsEnabled();
            }
        }, WAIT_CONDITION_TIMEOUT));

        stopGrid(1);

        final Ignite ig3 = startGrid(3);

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return ig3.cache(CACHE1).metrics().isStatisticsEnabled()
                    && !ig3.cache(CACHE2).metrics().isStatisticsEnabled();
            }
        }, WAIT_CONDITION_TIMEOUT));

        mxBeanCache1loc.disableStatistics();

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return !ig2.cache(CACHE1).metrics().isStatisticsEnabled()
                    && !ig3.cache(CACHE1).metrics().isStatisticsEnabled();
            }
        }, WAIT_CONDITION_TIMEOUT));

        mxBeanCache1.enableStatistics();
        mxBeanCache2.enableStatistics();

        // Start node 1 again.
        final Ignite ig4 = startGrid(1);

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return ig2.cache(CACHE1).metrics().isStatisticsEnabled()
                    && ig3.cache(CACHE1).metrics().isStatisticsEnabled()
                    && ig4.cache(CACHE1).metrics().isStatisticsEnabled()
                    && ig2.cache(CACHE2).metrics().isStatisticsEnabled()
                    && ig3.cache(CACHE2).metrics().isStatisticsEnabled()
                    && ig4.cache(CACHE2).metrics().isStatisticsEnabled();
            }
        }, WAIT_CONDITION_TIMEOUT));
    }
    /**
     * @throws Exception If failed.
     */
    public void testJmxNoPdsStatisticsEnable() throws Exception {
        testJmxStatisticsEnable(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testJmxPdsStatisticsEnable() throws Exception {
        testJmxStatisticsEnable(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheManagerStatisticsEnable() throws Exception {
        CacheManager mgr1 = Caching.getCachingProvider().getCacheManager();
        CacheManager mgr2 = Caching.getCachingProvider().getCacheManager();

        CacheConfiguration cfg1 = new CacheConfiguration()
            .setName(CACHE1)
            .setGroupName(GROUP)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC);

        mgr1.createCache(CACHE1, cfg1);

        CacheConfiguration cfg2 = new CacheConfiguration(cfg1)
            .setName(CACHE2)
            .setStatisticsEnabled(true);

        mgr1.createCache(CACHE2, cfg2);

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return !mgr1.getCache(CACHE1).getConfiguration(CacheConfiguration.class).isStatisticsEnabled()
                    && !mgr2.getCache(CACHE1).getConfiguration(CacheConfiguration.class).isStatisticsEnabled()
                    && mgr1.getCache(CACHE2).getConfiguration(CacheConfiguration.class).isStatisticsEnabled()
                    && mgr2.getCache(CACHE2).getConfiguration(CacheConfiguration.class).isStatisticsEnabled();
            }
        }, WAIT_CONDITION_TIMEOUT));

        mgr1.enableStatistics(CACHE1, true);
        mgr2.enableStatistics(CACHE2, false);

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return mgr1.getCache(CACHE1).getConfiguration(CacheConfiguration.class).isStatisticsEnabled()
                    && mgr2.getCache(CACHE1).getConfiguration(CacheConfiguration.class).isStatisticsEnabled()
                    && !mgr1.getCache(CACHE2).getConfiguration(CacheConfiguration.class).isStatisticsEnabled()
                    && !mgr2.getCache(CACHE2).getConfiguration(CacheConfiguration.class).isStatisticsEnabled();
            }
        }, WAIT_CONDITION_TIMEOUT));
    }
}
