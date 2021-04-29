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

import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.spi.metric.DoubleMetric;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class SystemMetricsTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * Checks that the process CPU load metric is positive.
     */
    @Test
    public void testCpuLoadMetric() {
        MetricRegistry sysReg = grid(0).context().metric().registry(GridMetricManager.SYS_METRICS);

        DoubleMetric cpuLoad = sysReg.doubleMetric(GridMetricManager.CPU_LOAD, GridMetricManager.CPU_LOAD_DESCRIPTION);

        double loadVal = cpuLoad.value();

        assertTrue("CPU Load is negative: " + loadVal, loadVal >= 0);
    }

    /**
     * Checks that the total physical memory node attribute has a positive value.
     */
    @Test
    public void testTotalSystemMemory() {
        long phyMem = (long)grid(0).context().nodeAttribute(IgniteNodeAttributes.ATTR_PHY_RAM);

        assertTrue("Total system memory size is negative: " + phyMem, phyMem >= 0);
    }
}
