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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheRebalancingEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.mxbean.CacheMetricsMXBean;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_REBALANCE_STATISTICS_TIME_INTERVAL;

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
    private static final String CACHE3 = "cache3";

    /** */
    private static final long REBALANCE_DELAY = 5_000;

    /** */
    private static final String GROUP = "group1";

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

        CacheConfiguration cfg1 = new CacheConfiguration()
            .setName(CACHE1)
            .setGroupName(GROUP)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC);

        CacheConfiguration cfg2 = new CacheConfiguration(cfg1)
            .setName(CACHE2);

        CacheConfiguration cfg3 = new CacheConfiguration()
            .setName(CACHE3)
            .setCacheMode(CacheMode.REPLICATED);

        cfg.setCacheConfiguration(cfg1, cfg2, cfg3);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testStatisticsEnableDisable() throws Exception {
        Ignite ig1 = startGrid(1);
        Ignite ig2 = startGrid(2);

        int KEYS = 1_000;

        CacheMetricsMXBean mxBean = mxBean(1, CACHE1);

        mxBean.enableStatistics();

        awaitPartitionMapExchange();

        assertEquals(true, ig1.cache(CACHE1).metrics().isStatisticsEnabled());
        assertEquals(true, ig2.cache(CACHE1).metrics().isStatisticsEnabled());


        Ignite ig3 = startGrid(3);

        awaitPartitionMapExchange();

        assertEquals(true, ig3.cache(CACHE1).metrics().isStatisticsEnabled());

        for (int i = 0; i < KEYS; i++)
            ig1.cache(CACHE1).put(i, i);

        long puts = mxBean.getCachePuts();
    }
}
