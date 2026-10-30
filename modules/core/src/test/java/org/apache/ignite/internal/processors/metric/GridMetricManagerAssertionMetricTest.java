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

package org.apache.ignite.internal.processors.metric;

import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.metric.GridMetricManager.ASSERTION_ERROR_CNT;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.SYS_METRICS;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests the ASSERTION_ERROR_CNT metric in the sys metrics registry.
 */
public class GridMetricManagerAssertionMetricTest extends GridCommonAbstractTest {

    /**
     * Verifies that the ASSERTION_ERROR_CNT counter increments
     * when an AssertionError is thrown.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testAssertionErrorMetric() throws Exception {
        try {
            IgniteEx grid = startGrid(0);

            MetricRegistry sysReg = grid.context().metric().registry(SYS_METRICS);

            assertNotNull(sysReg);

            LongMetric ctr = sysReg.findMetric(ASSERTION_ERROR_CNT);

            assertNotNull("Metric " + ASSERTION_ERROR_CNT + " not found", ctr);

            long init = ctr.value();

            // Throw an AssertionError.
            try {
                throw new AssertionError("test assertion");
            }
            catch (AssertionError e) {
                // Expected.
            }

            // Metric should increment (JFR events are delivered asynchronously).
            assertTrue(
                "Metric should increment after AssertionError",
                waitForCondition(() -> ctr.value() >= init + 1, getTestTimeout()));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Verifies that the ASSERTION_ERROR_CNT metric exposes proper name and description.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testMetricMetadata() throws Exception {
        try {
            IgniteEx grid = startGrid(0);

            MetricRegistry sysReg = grid.context().metric().registry(SYS_METRICS);

            LongMetric ctr = sysReg.findMetric(ASSERTION_ERROR_CNT);

            assertNotNull(ctr);

            assertEquals(SYS_METRICS + "." + ASSERTION_ERROR_CNT, ctr.name());

            assertEquals("Number of AssertionErrors thrown in this JVM.", ctr.description());
        }
        finally {
            stopAllGrids();
        }
    }
}
