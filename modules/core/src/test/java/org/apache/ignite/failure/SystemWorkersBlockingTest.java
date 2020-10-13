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

package org.apache.ignite.failure;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.internal.worker.WorkersRegistry;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.thread.IgniteThread;
import org.junit.Test;

/**
 * Tests the handling of long blocking operations in system-critical workers.
 */
public class SystemWorkersBlockingTest extends GridCommonAbstractTest {
    /** */
    private static final long SYSTEM_WORKER_BLOCKED_TIMEOUT = 1_000L;

    /** Handler latch. */
    private final CountDownLatch hndLatch = new CountDownLatch(1);

    /** Reference to failure error. */
    private final AtomicReference<Throwable> failureError = new AtomicReference<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        // Set small value for the test.
        cfg.setSystemWorkerBlockedTimeout(SYSTEM_WORKER_BLOCKED_TIMEOUT);

        AbstractFailureHandler failureHnd = new AbstractFailureHandler() {
            @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
                if (failureCtx.type() == FailureType.SYSTEM_WORKER_BLOCKED) {
                    failureError.set(failureCtx.error());

                    hndLatch.countDown();
                }

                return false;
            }
        };

        Set<FailureType> ignoredFailureTypes = new HashSet<>(failureHnd.getIgnoredFailureTypes());

        ignoredFailureTypes.remove(FailureType.SYSTEM_WORKER_BLOCKED);

        failureHnd.setIgnoredFailureTypes(ignoredFailureTypes);

        cfg.setFailureHandler(failureHnd);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBlockingWorker() throws Exception {
        IgniteEx ignite = startGrid(0);

        CountDownLatch blockLatch = new CountDownLatch(1);

        GridWorker worker = new GridWorker(ignite.name(), "test-worker", log) {
            @Override protected void body() throws InterruptedException {
                blockLatch.await();
            }
        };

        IgniteThread runner = null;
        try {
            runner = runWorker(worker);

            ignite.context().workersRegistry().register(worker);

            assertTrue(hndLatch.await(SYSTEM_WORKER_BLOCKED_TIMEOUT * 2, TimeUnit.MILLISECONDS));

            Throwable err = failureError.get();

            assertNotNull(err);
            assertTrue(err.getMessage() != null && err.getMessage().contains("test-worker"));
        }
        finally {
            if (runner != null) {
                blockLatch.countDown();

                runner.join(SYSTEM_WORKER_BLOCKED_TIMEOUT);
            }
        }
    }

    /**
     * Tests that repeatedly calling {@link WorkersRegistry#onIdle} in single registered {@link GridWorker}
     * doesn't lead to infinite loop.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSingleWorker_NotInInfiniteLoop() throws Exception {
        WorkersRegistry registry = new WorkersRegistry((w, e) -> {}, SYSTEM_WORKER_BLOCKED_TIMEOUT, log());

        CountDownLatch finishLatch = new CountDownLatch(1);

        GridWorker worker = new GridWorker("test", "test-worker", log, registry) {
            @Override protected void body() {
                while (!Thread.currentThread().isInterrupted()) {
                    onIdle();

                    LockSupport.parkNanos(1000);
                }

                finishLatch.countDown();
            }
        };

        IgniteThread runner = runWorker(worker);

        Thread.sleep(2 * SYSTEM_WORKER_BLOCKED_TIMEOUT);

        runner.interrupt();

        assertTrue(finishLatch.await(SYSTEM_WORKER_BLOCKED_TIMEOUT, TimeUnit.MILLISECONDS));
    }

    /**
     * @param worker Grid worker to run.
     * @return Thread, running worker.
     */
    private IgniteThread runWorker(GridWorker worker) throws IgniteInterruptedCheckedException {
        IgniteThread runner = new IgniteThread(worker);

        runner.start();

        GridTestUtils.waitForCondition(() -> worker.runner() != null, 100);

        return runner;
    }
}
