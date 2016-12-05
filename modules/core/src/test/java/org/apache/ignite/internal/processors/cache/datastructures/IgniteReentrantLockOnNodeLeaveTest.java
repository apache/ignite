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

package org.apache.ignite.internal.processors.cache.datastructures;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

/**
 * Cache reentrant lock hang test.
 * The test reproduces the problem for: https://issues.apache.org/jira/browse/IGNITE-4369.
 */
public class IgniteReentrantLockOnNodeLeaveTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES_CNT = 2;

    /** */
    private static final int WORKERS_PER_NODE = 16;

    /** */
    private static final int WORKER_TIMEOUT = 10;

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testEvents() throws Exception {
        startGrids(NODES_CNT);

        ExecutorService service = createExecutor(WORKERS_PER_NODE * 2);

        final CountDownLatch latch0 = new CountDownLatch(WORKERS_PER_NODE);
        final List<Worker> workersNode0 = new ArrayList(WORKERS_PER_NODE);

        final CountDownLatch latch1 = new CountDownLatch(WORKERS_PER_NODE);
        final List<Worker> workersNode1 = new ArrayList(WORKERS_PER_NODE);

        for (int i = 0; i < WORKERS_PER_NODE; i++) {
            {
                Worker workerNode0 = new Worker(i, grid(0), latch0);
                workersNode0.add(workerNode0);
                service.submit(workerNode0);
            }

            {
                Worker workerNode1 = new Worker(i, grid(1), latch1);
                workersNode1.add(workerNode1);
                service.submit(workerNode1);
            }
        }

        info("Stopping worker on node 1");

        for(Worker w : workersNode1)
            w.stop();

        boolean downNormally = latch1.await(WORKER_TIMEOUT, TimeUnit.SECONDS);

        if (!downNormally)
            Assert.fail("Workers are not ready, missing:" + latch1.getCount());

        stopGrid(1);

        info("Stopping worker on node 0");

        for(Worker w : workersNode0)
            w.stop();

        boolean normally = latch0.await(WORKER_TIMEOUT, TimeUnit.SECONDS);
        if (!normally)
            Assert.fail("Workers are not ready, missing: " + latch0.getCount());
    }

    /**
     * @param casesCount Count.
     * @return ExecutorService.
     */
    private ExecutorService createExecutor(final int casesCount) {
        ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat(">>JUnit-Test-Worker-%d").build();
        return Executors.newFixedThreadPool(casesCount * 2, factory);
    }

    /**
     *
     */
    private static class Worker implements Runnable {
        /** Index. */
        private final int idx;
        /** Node. */
        private final Ignite node;
        /** Stopped. */
        private volatile boolean stopped;
        /** Latch. */
        private final CountDownLatch latch;

        /**
         * @param idx Worker number.
         * @param node Ignite.
         * @param latch Latch.
         */
        public Worker(final int idx, final Ignite node, final CountDownLatch latch) {
            this.idx = idx;
            this.node = node;
            this.latch = latch;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                while (!stopped) {
                    IgniteLock lock = node.reentrantLock("Lock_" + idx,
                        false, false, true);

                    lock.lock();

                    try {
                        Thread.sleep(2);
                    }
                    catch (InterruptedException e) {
                        // No-op.
                    }
                    finally {
                        lock.unlock();
                    }
                }
            } catch (Throwable e) {
                System.err.println("Error occured in worker " + this.idx + " on node " + this.node.name());
                e.printStackTrace();
            }
            latch.countDown();
        }

        /**
         *
         */
        public void stop() {
            stopped = true;
        }
    }
}
