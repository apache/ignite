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

package org.apache.ignite.internal.processors.performancestatistics;

import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.mxbean.PerformanceStatisticsMBean;
import org.junit.Test;

/** Tests {@link PerformanceStatisticsMBeanImpl}. */
public class PerformanceStatisticsMBeanSelfTest extends AbstractPerformanceStatisticsTest {
    /** @throws Exception If failed. */
    @Test
    public void testStartStop() throws Exception {
        IgniteEx srv0 = startGrid(0);
        IgniteEx srv1 = startGrid(1);

        PerformanceStatisticsMBean statMBean0 = statisticsMBean(srv0.name());
        PerformanceStatisticsMBean statMBean1 = statisticsMBean(srv1.name());

        assertFalse(statMBean0.started());
        assertFalse(statMBean1.started());

        statMBean0.start();

        waitForStatisticsEnabled(true);

        assertTrue(statMBean0.started());
        assertTrue(statMBean1.started());

        statMBean0.stop();

        waitForStatisticsEnabled(false);

        assertFalse(statMBean0.started());
        assertFalse(statMBean1.started());
    }
}
