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
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.CacheMetricsMXBean;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
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
    private CacheMetricsMXBean mxBean(int nodeIdx, String cacheName) throws MalformedObjectNameException {
        ObjectName mbeanName = U.makeCacheMBeanName(getTestIgniteInstanceName(nodeIdx), cacheName,
            CacheClusterMetricsMXBeanImpl.class.getName());

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
     * @throws Exception If failed.
     */
    public void testStatisticsEnableDisable() throws Exception {
        persistence = false;

        Ignite ig1 = startGrid(1);
        Ignite ig2 = startGrid(2);

        CacheConfiguration cacheCfg2 = new CacheConfiguration(ig1.cache(CACHE1).getConfiguration(
            CacheConfiguration.class));

        cacheCfg2.setName(CACHE2);
        cacheCfg2.setStatisticsEnabled(true);

        ig1.getOrCreateCache(cacheCfg2);

        CacheMetricsMXBean mxBeanCache1 = mxBean(1, CACHE1);
        CacheMetricsMXBean mxBeanCache2 = mxBean(1, CACHE2);

        mxBeanCache1.enableStatistics();
        mxBeanCache2.disableStatistics();

        awaitPartitionMapExchange();

        assertTrue(ig1.cache(CACHE1).metrics().isStatisticsEnabled());
        assertTrue(ig2.cache(CACHE1).metrics().isStatisticsEnabled());
        assertFalse(ig1.cache(CACHE2).metrics().isStatisticsEnabled());
        assertFalse(ig2.cache(CACHE2).metrics().isStatisticsEnabled());

        Ignite ig3 = startGrid(3);

        assertTrue(ig3.cache(CACHE1).metrics().isStatisticsEnabled());
        assertFalse(ig3.cache(CACHE2).metrics().isStatisticsEnabled());
    }

    /**
     * @throws Exception If failed.
     */
    public void testPdsStatisticsEnableDisable() throws Exception {
        persistence = true;

        Ignite ig1 = startGrid(1);
        Ignite ig2 = startGrid(2);

        CacheConfiguration cacheCfg2 = new CacheConfiguration(ig1.cache(CACHE1).getConfiguration(
            CacheConfiguration.class));

        cacheCfg2.setName(CACHE2);
        cacheCfg2.setStatisticsEnabled(false);

        ig1.getOrCreateCache(cacheCfg2);

        CacheMetricsMXBean mxBeanCache1 = mxBean(1, CACHE1);
        CacheMetricsMXBean mxBeanCache2 = mxBean(1, CACHE2);

        mxBeanCache1.enableStatistics();
        mxBeanCache2.disableStatistics();

        awaitPartitionMapExchange();

        assertTrue(ig1.cache(CACHE1).metrics().isStatisticsEnabled());
        assertTrue(ig2.cache(CACHE1).metrics().isStatisticsEnabled());
        assertFalse(ig1.cache(CACHE2).metrics().isStatisticsEnabled());
        assertFalse(ig2.cache(CACHE2).metrics().isStatisticsEnabled());

        stopGrid(2);

        mxBeanCache1.disableStatistics();
        mxBeanCache2.enableStatistics();

        ig2 = startGrid(2);

        assertFalse(ig1.cache(CACHE1).metrics().isStatisticsEnabled());
        assertFalse(ig2.cache(CACHE1).metrics().isStatisticsEnabled());
        assertTrue(ig1.cache(CACHE2).metrics().isStatisticsEnabled());
        assertTrue(ig2.cache(CACHE2).metrics().isStatisticsEnabled());
    }
}
