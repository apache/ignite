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

package org.apache.ignite.internal.metric;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.thread.pool.IgniteStripedExecutor;
import org.apache.ignite.spi.systemview.view.StripedExecutorTaskView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.junit.Test;

import static org.apache.ignite.internal.processors.pool.PoolProcessor.STREAM_POOL_QUEUE_VIEW;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.SYS_POOL_QUEUE_VIEW;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** Tests for {@link SystemView} for executors. */
public class SystemViewExecutorsTest extends SystemViewAbstractTest {
    /** */
    @Test
    public void testStripedExecutors() throws Exception {
        try (IgniteEx g = startGrid(0)) {
            checkStripeExecutorView(g.context().pools().getStripedExecutorService(),
                g.context().systemView().view(SYS_POOL_QUEUE_VIEW),
                "sys");

            checkStripeExecutorView(g.context().pools().getDataStreamerExecutorService(),
                g.context().systemView().view(STREAM_POOL_QUEUE_VIEW),
                "data-streamer");
        }
    }

    /**
     * Checks striped executor system view.
     *
     * @param execSvc Striped executor.
     * @param view System view.
     * @param poolName Executor name.
     */
    private void checkStripeExecutorView(IgniteStripedExecutor execSvc, SystemView<StripedExecutorTaskView> view,
        String poolName) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        execSvc.execute(0, new TestRunnable(latch, 0));
        execSvc.execute(0, new TestRunnable(latch, 1));
        execSvc.execute(1, new TestRunnable(latch, 2));
        execSvc.execute(1, new TestRunnable(latch, 3));

        try {
            boolean res = waitForCondition(() -> view.size() == 2, 5_000);

            assertTrue(res);

            Iterator<StripedExecutorTaskView> iter = view.iterator();

            assertTrue(iter.hasNext());

            StripedExecutorTaskView row0 = iter.next();

            assertEquals(0, row0.stripeIndex());
            assertEquals(TestRunnable.class.getSimpleName() + '1', row0.description());
            assertEquals(poolName + "-stripe-0", row0.threadName());
            assertEquals(TestRunnable.class.getName(), row0.taskName());

            assertTrue(iter.hasNext());

            StripedExecutorTaskView row1 = iter.next();

            assertEquals(1, row1.stripeIndex());
            assertEquals(TestRunnable.class.getSimpleName() + '3', row1.description());
            assertEquals(poolName + "-stripe-1", row1.threadName());
            assertEquals(TestRunnable.class.getName(), row1.taskName());
        }
        finally {
            latch.countDown();
        }
    }

    /** Test runnable. */
    public static class TestRunnable implements Runnable {
        /** */
        private final CountDownLatch latch;

        /** */
        private final int idx;

        /** */
        public TestRunnable(CountDownLatch latch, int idx) {
            this.latch = latch;
            this.idx = idx;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                latch.await(5, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return getClass().getSimpleName() + idx;
        }
    }
}
