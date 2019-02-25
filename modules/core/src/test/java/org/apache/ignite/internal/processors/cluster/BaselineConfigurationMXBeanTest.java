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

package org.apache.ignite.internal.processors.cluster;

import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.cluster.DistributedBaselineConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.BaselineConfigurationMXBean;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class BaselineConfigurationMXBeanTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void testTimeoutAndEnabledFlag() throws Exception {
        Ignite ignite = startGrid();

        try {
            DistributedBaselineConfiguration bltConf = ignite.cluster().baselineConfiguration();

            BaselineConfigurationMXBean bltMxBean = bltMxBean();

            assertTrue(bltConf.isBaselineAutoAdjustEnabled());
            assertTrue(bltMxBean.isAutoAdjustmentEnabled());

            assertEquals(0L, bltConf.getBaselineAutoAdjustTimeout());
            assertEquals(0L, bltMxBean.getAutoAdjustmentTimeout());

            bltConf.updateBaselineAutoAdjustEnabledAsync(false).get();
            assertFalse(bltMxBean.isAutoAdjustmentEnabled());

            bltConf.updateBaselineAutoAdjustTimeoutAsync(30_000L).get();
            assertEquals(30_000L, bltMxBean.getAutoAdjustmentTimeout());

            bltMxBean.setAutoAdjustmentEnabled(false);
            assertFalse(bltConf.isBaselineAutoAdjustEnabled());

            bltMxBean.setAutoAdjustmentTimeout(60_000L);
            assertEquals(60_000L, bltConf.getBaselineAutoAdjustTimeout());
        }
        finally {
            stopGrid(0);
        }
    }

    /**
     *
     */
    private BaselineConfigurationMXBean bltMxBean() throws Exception {
        ObjectName mBeanName = U.makeMBeanName(getTestIgniteInstanceName(), "Baseline",
            BaselineConfigurationMXBeanImpl.class.getSimpleName());

        MBeanServer mBeanSrv = ManagementFactory.getPlatformMBeanServer();

        assertTrue(mBeanSrv.isRegistered(mBeanName));

        Class<BaselineConfigurationMXBean> itfCls = BaselineConfigurationMXBean.class;

        return MBeanServerInvocationHandler.newProxyInstance(mBeanSrv, mBeanName, itfCls, true);
    }
}
