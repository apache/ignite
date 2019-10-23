/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.stat;

import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.IoStatisticsMetricsMXBean;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.metric.IoStatisticsHolderIndex.HASH_PK_IDX_NAME;

/**
 *
 */
public class IoStatisticsMetricsLocalMxBeanImplIllegalArgumentsTest extends GridCommonAbstractTest {
    /** */
    public static final String MEMORY_CACHE_NAME = "inmemory";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setCacheConfiguration(new CacheConfiguration(MEMORY_CACHE_NAME));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        Ignite ignite = startGrid(0);

        ignite.cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids(true);

        cleanPersistenceDir();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testNonExistingCachesMetrics() throws Exception {
        IoStatisticsMetricsMXBean bean = ioStatMXBean();

        String nonExistingCache = "non-existing-cache";
        String nonExistingIdx = "non-existing-index";

        assertNull(bean.getIndexLeafLogicalReads(nonExistingCache, HASH_PK_IDX_NAME));
        assertNull(bean.getIndexLeafPhysicalReads(nonExistingCache, HASH_PK_IDX_NAME));
        assertNull(bean.getIndexInnerLogicalReads(nonExistingCache, HASH_PK_IDX_NAME));
        assertNull(bean.getIndexInnerPhysicalReads(nonExistingCache, HASH_PK_IDX_NAME));
        assertNull(bean.getIndexLogicalReads(nonExistingCache, HASH_PK_IDX_NAME));
        assertNull(bean.getIndexPhysicalReads(nonExistingCache, HASH_PK_IDX_NAME));
        assertNull(bean.getCacheGroupLogicalReads(nonExistingCache));
        assertNull(bean.getCacheGroupPhysicalReads(nonExistingCache));

        assertNull(bean.getIndexLeafLogicalReads(MEMORY_CACHE_NAME, nonExistingIdx));
        assertNull(bean.getIndexLeafPhysicalReads(MEMORY_CACHE_NAME, nonExistingIdx));
        assertNull(bean.getIndexInnerLogicalReads(MEMORY_CACHE_NAME, nonExistingIdx));
        assertNull(bean.getIndexInnerPhysicalReads(MEMORY_CACHE_NAME, nonExistingIdx));
        assertNull(bean.getIndexLogicalReads(MEMORY_CACHE_NAME, nonExistingIdx));
        assertNull(bean.getIndexPhysicalReads(MEMORY_CACHE_NAME, nonExistingIdx));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testNullArguments() throws Exception {
        IoStatisticsMetricsMXBean bean = ioStatMXBean();

        assertNull(bean.getIndexLeafLogicalReads(null, HASH_PK_IDX_NAME));
        assertNull(bean.getIndexInnerLogicalReads(null, HASH_PK_IDX_NAME));
        assertNull(bean.getIndexLeafPhysicalReads(null, HASH_PK_IDX_NAME));
        assertNull(bean.getIndexInnerPhysicalReads(null, HASH_PK_IDX_NAME));
        assertNull(bean.getIndexStatistics(null, HASH_PK_IDX_NAME));

        assertNull(bean.getIndexLeafLogicalReads(MEMORY_CACHE_NAME, null));
        assertNull(bean.getIndexInnerLogicalReads(MEMORY_CACHE_NAME, null));
        assertNull(bean.getIndexLeafPhysicalReads(MEMORY_CACHE_NAME, null));
        assertNull(bean.getIndexInnerPhysicalReads(MEMORY_CACHE_NAME, null));
        assertNull(bean.getIndexStatistics(MEMORY_CACHE_NAME, null));

        assertNull(bean.getCacheGroupLogicalReads(null));
        assertNull(bean.getCacheGroupPhysicalReads(null));
        assertNull(bean.getCacheGroupStatistics(null));
    }

    /**
     * @return IO statistics MX bean for node with given index.
     * @throws Exception In case of failure.
     */
    private IoStatisticsMetricsMXBean ioStatMXBean() throws Exception {
        ObjectName mbeanName = U.makeMBeanName(getTestIgniteInstanceName(0), "IOMetrics",
            IoStatisticsMetricsLocalMXBeanImpl.class.getSimpleName());

        MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();

        if (!mbeanSrv.isRegistered(mbeanName))
            fail("MBean is not registered: " + mbeanName.getCanonicalName());

        return MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName, IoStatisticsMetricsMXBean.class, false);
    }
}
