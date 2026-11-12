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

package org.apache.ignite.internal.thread.pool;

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_STARVATION_CHECK_INTERVAL;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.THREAD_POOLS;
import static org.apache.ignite.internal.thread.pool.IgniteStripedExecutor.STARVATION_EVENTS_CNT;

/**
 * Tests that the data streamer striped pool is checked by the periodic starvation detector and that its
 * {@code StarvationEventsCount} metric is updated without reading {@code DetectStarvation}.
 */
@WithSystemProperty(key = IGNITE_STARVATION_CHECK_INTERVAL, value = "100")
public class DataStreamerStarvationMetricTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setFailureDetectionTimeout(500);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** */
    @Test
    public void testStarvationEventsCountUpdatedByPeriodicCheck() throws Exception {
        IgniteEx ignite = startGrid(0);

        LongMetric cnt = ignite.context().metric().registry(metricName(THREAD_POOLS, "GridDataStreamExecutor"))
            .findMetric(STARVATION_EVENTS_CNT);

        assertNotNull("StarvationEventsCount is not registered for data streamer", cnt);
        assertEquals(0L, cnt.value());

        CountDownLatch latch = new CountDownLatch(1);

        try {
            ignite.context().pools().getDataStreamerExecutorService().execute(0, () -> U.awaitQuiet(latch));

            assertTrue("StarvationEventsCount is not updated by the periodic check",
                GridTestUtils.waitForCondition(() -> cnt.value() == 1, 10_000));
        }
        finally {
            latch.countDown();
        }
    }
}
