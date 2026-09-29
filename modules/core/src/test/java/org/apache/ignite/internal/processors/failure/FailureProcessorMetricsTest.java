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

import java.util.Collections;
import java.util.Set;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
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
import static org.apache.ignite.internal.processors.failure.FailureProcessor.IGNORED_FAILURES_PREFIX;

/**
 * Tests that the failure processor counts suppressed (ignored) failures per failure type via dedicated metrics
 * of the form {@code failure.ignored.<FailureType>}.
 */
@WithSystemProperty(key = IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE, value = "false")
public class FailureProcessorMetricsTest extends GridCommonAbstractTest {
    /** */
    private static final String ERR_MSG = "Failure context error";

    /** */
    private Set<FailureType> ignoredTypes;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TestFailureHandler hnd = new TestFailureHandler(false);

        hnd.setIgnoredFailureTypes(ignoredTypes);

        cfg.setFailureHandler(hnd);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Tests that per-type ignored failures count metrics start at zero and are incremented per each suppressed
     * failure of the matching type, while processed (not ignored) failures do not affect any counter.
     */
    @Test
    public void testPerTypeIgnoredFailuresCountMetrics() throws Exception {
        ignoredTypes = Set.of(SYSTEM_CRITICAL_OPERATION_TIMEOUT, SYSTEM_WORKER_BLOCKED);

        IgniteEx ignite = startGrids(2);

        LongMetric criticalTimeoutCnt = ignoredFailuresMetric(ignite, SYSTEM_CRITICAL_OPERATION_TIMEOUT);
        LongMetric workerBlockedCnt = ignoredFailuresMetric(ignite, SYSTEM_WORKER_BLOCKED);

        assertEquals(0, criticalTimeoutCnt.value());
        assertEquals(0, workerBlockedCnt.value());

        ignite.context().failure().process(new FailureContext(SYSTEM_CRITICAL_OPERATION_TIMEOUT, new Throwable(ERR_MSG)));

        assertEquals(1, criticalTimeoutCnt.value());
        assertEquals(0, workerBlockedCnt.value());

        // A processed (not ignored) failure must not affect any ignored counter.
        ignite.context().failure().process(new FailureContext(SEGMENTATION, new Throwable(ERR_MSG)));

        assertEquals(1, criticalTimeoutCnt.value());
        assertEquals(0, workerBlockedCnt.value());

        ignite.context().failure().process(new FailureContext(SYSTEM_WORKER_BLOCKED, new Throwable(ERR_MSG)));

        assertEquals(1, criticalTimeoutCnt.value());
        assertEquals(1, workerBlockedCnt.value());

        ignite.context().failure().process(new FailureContext(SYSTEM_CRITICAL_OPERATION_TIMEOUT, new Throwable(ERR_MSG)));

        assertEquals(2, criticalTimeoutCnt.value());
        assertEquals(1, workerBlockedCnt.value());

        assertEquals(0, ignoredFailuresMetric(grid(1), SYSTEM_CRITICAL_OPERATION_TIMEOUT).value());
        assertEquals(0, ignoredFailuresMetric(grid(1), SYSTEM_WORKER_BLOCKED).value());
    }

    /** Tests that no {@code failure.ignored.*} metrics are registered when the handler ignores no failure types. */
    @Test
    public void testNoMetricsRegisteredWhenNothingIgnored() throws Exception {
        ignoredTypes = Collections.emptySet();

        IgniteEx ignite = startGrid(0);

        for (FailureType t : FailureType.values())
            assertNull(ignoredFailuresMetric(ignite, t));
    }

    /** */
    private LongMetric ignoredFailuresMetric(IgniteEx ignite, FailureType type) {
        return ignite.context().metric().registry(FAILURE_METRICS).findMetric(IGNORED_FAILURES_PREFIX + '.' + type.name());
    }
}
