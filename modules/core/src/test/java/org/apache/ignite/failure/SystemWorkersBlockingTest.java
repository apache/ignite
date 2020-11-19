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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests the handling of long blocking operations in system-critical workers.
 */
public class SystemWorkersBlockingTest extends GridCommonAbstractTest {
    /** */
    private static final long SYSTEM_WORKER_BLOCKED_TIMEOUT = 1_000L;

    /** Handler latch. */
    private final CountDownLatch hndLatch = new CountDownLatch(1);

    /** Blocking thread latch. */
    private final CountDownLatch blockLatch = new CountDownLatch(1);

    /** Worker executor. */
    private final ExecutorService workerExecutor = Executors.newSingleThreadExecutor();

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

        blockLatch.countDown();

        if (workerExecutor.isTerminated()) {
            workerExecutor.shutdownNow();
            workerExecutor.awaitTermination(2 * SYSTEM_WORKER_BLOCKED_TIMEOUT, TimeUnit.MILLISECONDS);
        }

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBlockingWorker() throws Exception {
        IgniteEx ignite = startGrid(0);

        GridWorker worker = new LatchingGridWorker(ignite);

        runWorker(worker);

        ignite.context().workersRegistry().register(worker);

        assertTrue(hndLatch.await(ignite.configuration().getFailureDetectionTimeout() * 2,
            TimeUnit.MILLISECONDS));

        Throwable blockedExeption = failureError.get();

        assertNotNull(blockedExeption);

        assertTrue(Arrays.stream(blockedExeption.getStackTrace()).anyMatch(
            e -> CountDownLatch.class.getName().equals(e.getClassName())));
        assertTrue(Arrays.stream(blockedExeption.getStackTrace()).anyMatch(
            e -> LatchingGridWorker.class.getName().equals(e.getClassName())));
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

        runWorker(worker);

        Thread.sleep(2 * SYSTEM_WORKER_BLOCKED_TIMEOUT);

        workerExecutor.shutdownNow();

        assertTrue(workerExecutor.awaitTermination(SYSTEM_WORKER_BLOCKED_TIMEOUT, TimeUnit.MILLISECONDS));
    }

    /**
     * Run worker and wait for its initialization.
     *
     * @param worker GridWorker to run.
     * @throws IgniteInterruptedCheckedException If wait is interrupted.
     */
    private void runWorker(GridWorker worker) throws IgniteInterruptedCheckedException {
        workerExecutor.execute(worker);

        GridTestUtils.waitForCondition(() -> worker.runner() != null, 100);
    }

    /** */
    private class LatchingGridWorker extends GridWorker {
        /** */
        public LatchingGridWorker(IgniteEx ignite) {
            super(ignite.name(), "test-worker", GridAbstractTest.log);
        }

        /** */
        @Override protected void body() throws InterruptedException {
            blockLatch.await();
        }
    }
}
