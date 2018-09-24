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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.thread.IgniteThread;

/**
 * Tests the handling of long blocking operations in system-critical workers.
 */
public class SystemWorkersBlockingTest extends GridCommonAbstractTest {
    /** Handler latch. */
    private static volatile CountDownLatch hndLatch;

    /** */
    private static final long FAILURE_DETECTION_TIMEOUT = 5_000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setFailureHandler(new AbstractFailureHandler() {
            @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
                hndLatch.countDown();

                return false;
            }
        });

        cfg.setFailureDetectionTimeout(FAILURE_DETECTION_TIMEOUT);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        hndLatch = new CountDownLatch(1);

        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testBlockingWorker() throws Exception {
        IgniteEx ignite = grid(0);

        GridWorker worker = new GridWorker(ignite.name(), "test-worker", log) {
            @Override protected void body() throws InterruptedException {
                Thread.sleep(Long.MAX_VALUE);
            }
        };

        new IgniteThread(worker).start();

        while (worker.runner() == null)
            Thread.sleep(10);

        ignite.context().workersRegistry().register(worker);

        assertTrue(hndLatch.await(ignite.configuration().getFailureDetectionTimeout() * 2, TimeUnit.MILLISECONDS));

        Thread runner = worker.runner();

        runner.interrupt();
        runner.join(1000);

        assertFalse(runner.isAlive());
    }
}
