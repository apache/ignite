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

package org.apache.ignite.internal.processors.failure;

import java.util.Set;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.TestFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.failure.FailureType.SEGMENTATION;
import static org.apache.ignite.failure.FailureType.SYSTEM_CRITICAL_OPERATION_TIMEOUT;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_BLOCKED;
import static org.apache.ignite.internal.processors.failure.FailureProcessor.FAILURE_METRICS;
import static org.apache.ignite.internal.processors.failure.FailureProcessor.IGNORED_FAILURES_CNT;

/** Tests that the failure processor counts suppressed (ignored) failures via a dedicated metric. */
@WithSystemProperty(key = IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE, value = "false")
public class FailureProcessorMetricsTest extends GridCommonAbstractTest {
    /** */
    private static final String ERR_MSG = "Failure context error";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TestFailureHandler hnd = new TestFailureHandler(false);

        hnd.setIgnoredFailureTypes(Set.of(SYSTEM_CRITICAL_OPERATION_TIMEOUT, SYSTEM_WORKER_BLOCKED));

        cfg.setFailureHandler(hnd);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Tests that the ignored failures count metric starts at zero and is incremented per each suppressed failure,
     * while processed (not ignored) failures do not affect it.
     */
    @Test
    public void testIgnoredFailuresCountMetric() throws Exception {
        IgniteEx ignite = startGrids(2);

        LongMetric ignoredFailuresCnt = ignoredFailuresMetric(ignite);

        assertEquals(0, ignoredFailuresCnt.value());

        ignite.context().failure().process(new FailureContext(SYSTEM_CRITICAL_OPERATION_TIMEOUT, new Throwable(ERR_MSG)));

        assertEquals(1, ignoredFailuresCnt.value());

        // A processed (not ignored) failure must not affect the ignored failures count.
        ignite.context().failure().process(new FailureContext(SEGMENTATION, new Throwable(ERR_MSG)));

        assertEquals(1, ignoredFailuresCnt.value());

        ignite.context().failure().process(new FailureContext(SYSTEM_WORKER_BLOCKED, new Throwable(ERR_MSG)));

        assertEquals(2, ignoredFailuresCnt.value());
        assertEquals(0, ignoredFailuresMetric(grid(1)).value());
    }

    /** */
    private LongMetric ignoredFailuresMetric(IgniteEx ignite) {
        return ignite.context().metric().registry(FAILURE_METRICS).findMetric(IGNORED_FAILURES_CNT);
    }
}
