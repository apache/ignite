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

import com.google.common.collect.ImmutableSet;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.failure.TestFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.failure.FailureType.SYSTEM_CRITICAL_OPERATION_TIMEOUT;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_BLOCKED;
import static org.apache.ignite.internal.util.IgniteUtils.THREAD_DUMP_MSG;

/**
 * Tests for throttling thread dumps during handling failures.
 */
@WithSystemProperty(key = IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE, value = "true")
public class FailureProcessorThreadDumpThrottlingTest extends GridCommonAbstractTest {
    /** Test logger. */
    private final ListeningTestLogger testLog = new ListeningTestLogger(log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TestFailureHandler hnd = new TestFailureHandler(false);

        hnd.setIgnoredFailureTypes(ImmutableSet.of(FailureType.SYSTEM_CRITICAL_OPERATION_TIMEOUT, SYSTEM_WORKER_BLOCKED));

        cfg.setFailureHandler(hnd);

        cfg.setGridLogger(testLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        testLog.clearListeners();

        stopAllGrids();

        super.afterTest();
    }

    /**
     * Tests that thread dumps will not get if {@code IGNITE_DUMP_THREADS_ON_FAILURE == false}.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE, value = "false")
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE_THROTTLING_TIMEOUT, value = "0")
    public void testNoThreadDumps() throws Exception {
        LogListener lsnr = LogListener.matches(THREAD_DUMP_MSG).times(0).build();

        testLog.registerListener(lsnr);

        IgniteEx ignite = ignite(0);

        FailureContext failureCtx =
                new FailureContext(SYSTEM_WORKER_BLOCKED, new Throwable("Failure context error"));

        for (int i = 0; i < 2; i++)
            ignite.context().failure().process(failureCtx);

        assertTrue(lsnr.check());
    }

    /**
     * Tests that thread dumps will get for every failure for disabled throttling.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE, value = "true")
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE_THROTTLING_TIMEOUT, value = "0")
    public void testNoThrottling() throws Exception {
        LogListener lsnr = LogListener.matches(THREAD_DUMP_MSG).times(2).build();

        testLog.registerListener(lsnr);

        IgniteEx ignite = ignite(0);

        FailureContext failureCtx =
                new FailureContext(SYSTEM_WORKER_BLOCKED, new Throwable("Failure context error"));

        for (int i = 0; i < 2; i++)
            ignite.context().failure().process(failureCtx);

        assertTrue(lsnr.check());
    }

    /**
     * Tests that thread dumps will be throttled and will be generated again after timeout exceeded.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE, value = "true")
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE_THROTTLING_TIMEOUT, value = "3000")
    public void testThrottling() throws Exception {
        LogListener dumpLsnr = LogListener.matches(THREAD_DUMP_MSG).times(2).build();
        LogListener throttledLsnr = LogListener.matches("Thread dump is hidden").times(2).build();

        testLog.registerListener(dumpLsnr);
        testLog.registerListener(throttledLsnr);

        IgniteEx ignite = ignite(0);

        FailureContext failureCtx =
                new FailureContext(SYSTEM_WORKER_BLOCKED, new Throwable("Failure context error"));

        for (int i = 0; i < 2; i++)
            ignite.context().failure().process(failureCtx);

        U.sleep(3000);

        for (int i = 0; i < 2; i++)
            ignite.context().failure().process(failureCtx);

        assertTrue(dumpLsnr.check());
        assertTrue(throttledLsnr.check());
    }

    /**
     * Tests that thread dumps will be throttled per failure type and will be generated again after timeout exceeded.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE, value = "true")
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE_THROTTLING_TIMEOUT, value = "3000")
    public void testThrottlingPerFailureType() throws Exception {
        LogListener dumpLsnr = LogListener.matches(THREAD_DUMP_MSG).times(4).build();
        LogListener throttledLsnr = LogListener.matches("Thread dump is hidden").times(4).build();

        testLog.registerListener(dumpLsnr);
        testLog.registerListener(throttledLsnr);

        IgniteEx ignite = ignite(0);

        FailureContext workerBlockedFailureCtx =
                new FailureContext(SYSTEM_WORKER_BLOCKED, new Throwable("Failure context error"));

        FailureContext opTimeoutFailureCtx =
                new FailureContext(SYSTEM_CRITICAL_OPERATION_TIMEOUT, new Throwable("Failure context error"));

        for (int i = 0; i < 2; i++) {
            ignite.context().failure().process(workerBlockedFailureCtx);

            ignite.context().failure().process(opTimeoutFailureCtx);
        }

        U.sleep(3000);

        for (int i = 0; i < 2; i++) {
            ignite.context().failure().process(workerBlockedFailureCtx);

            ignite.context().failure().process(opTimeoutFailureCtx);
        }

        assertTrue(dumpLsnr.check());
        assertTrue(throttledLsnr.check());
    }

    /**
     * Tests that default thread dump trhottling timeout equals failure detection timeout.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE, value = "true")
    public void testDefaultThrottlingTimeout() throws Exception {
        IgniteEx ignite = ignite(0);

        assertEquals(
                ignite.context().failure().dumpThreadsTrottlingTimeout,
                ignite.configuration().getFailureDetectionTimeout().longValue()
        );
    }
}
