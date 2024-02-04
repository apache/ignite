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

package org.apache.ignite.internal.processors.jobmetrics;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.job.GridJobProcessor;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.collision.CollisionContext;
import org.apache.ignite.spi.collision.CollisionExternalListener;
import org.apache.ignite.spi.collision.CollisionJobContext;
import org.apache.ignite.spi.collision.CollisionSpi;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.processors.job.GridJobProcessor.ACTIVE;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.CANCELED;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.EXECUTION_TIME;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.FINISHED;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.JOBS_METRICS;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.REJECTED;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.STARTED;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.WAITING;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.WAITING_TIME;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Grid job metrics processor load test.
 */
public class GridJobMetricsSelfTest extends GridCommonAbstractTest {
    /** */
    public static final long TIMEOUT = 10_000;

    /** */
    private static volatile CountDownLatch latch;

    /** Test correct calculation of rejected and waiting metrics of the {@link GridJobProcessor}. */
    @Test
    public void testGridJobWaitingRejectedMetrics() throws Exception {
        latch = new CountDownLatch(1);

        GridTestCollision collisioinSpi = new GridTestCollision();

        IgniteConfiguration cfg = getConfiguration()
            .setCollisionSpi(collisioinSpi);

        try (IgniteEx g = startGrid(cfg)) {
            MetricRegistry mreg = g.context().metric().registry(JOBS_METRICS);

            LongMetric started = mreg.findMetric(STARTED);
            LongMetric active = mreg.findMetric(ACTIVE);
            LongMetric waiting = mreg.findMetric(WAITING);
            LongMetric canceled = mreg.findMetric(CANCELED);
            LongMetric rejected = mreg.findMetric(REJECTED);
            LongMetric finished = mreg.findMetric(FINISHED);
            LongMetric totalExecutionTime = mreg.findMetric(EXECUTION_TIME);
            LongMetric totalWaitingTime = mreg.findMetric(WAITING_TIME);

            assertNotNull(started);
            assertNotNull(active);
            assertNotNull(waiting);
            assertNotNull(canceled);
            assertNotNull(rejected);
            assertNotNull(finished);
            assertNotNull(totalExecutionTime);
            assertNotNull(totalWaitingTime);

            assertEquals(0, started.value());
            assertEquals(0, active.value());
            assertEquals(0, waiting.value());
            assertEquals(0, canceled.value());
            assertEquals(0, rejected.value());
            assertEquals(0, finished.value());
            assertEquals(0, totalExecutionTime.value());
            assertEquals(0, totalWaitingTime.value());

            SimplestTask task1 = new SimplestTask();
            SimplestTask task2 = new SimplestTask();
            SimplestTask task3 = new SimplestTask();

            task1.block = true;
            task2.block = true;
            task3.block = true;

            // Task will become "waiting", because of CollisionSpi implementation.
            ComputeTaskFuture<?> fut1 = g.compute().executeAsync(task1, 1);
            ComputeTaskFuture<?> fut2 = g.compute().executeAsync(task2, 1);
            ComputeTaskFuture<?> fut3 = g.compute().executeAsync(task3, 1);

            assertEquals(0, started.value());
            assertEquals(0, active.value());
            assertEquals(3, waiting.value());
            assertEquals(0, canceled.value());
            assertEquals(0, rejected.value());
            assertEquals(0, finished.value());

            // Activating 2 of 3 jobs. Rejecting 1 of them.
            Iterator<CollisionJobContext> iter = collisioinSpi.jobs.values().iterator();

            iter.next().cancel();

            assertEquals(1, rejected.value());

            Thread.sleep(100); // Sleeping to make sure totalWaitingTime will become more the zero.

            iter.next().activate();
            iter.next().activate();

            boolean res = waitForCondition(() -> active.value() > 0, TIMEOUT);

            assertTrue(res);
            assertTrue("Waiting time should be greater then zero.", totalWaitingTime.value() > 0);

            Thread.sleep(100); // Sleeping to make sure totalExecutionTime will become more the zero.

            latch.countDown();

            res = waitForCondition(() -> fut1.isDone() && fut2.isDone() && fut3.isDone(), TIMEOUT);

            assertTrue(res);

            res = waitForCondition(() -> finished.value() == 3, TIMEOUT);

            assertTrue(res);

            assertTrue("Execution time should be greater then zero.", totalExecutionTime.value() > 0);
        }
    }

    /** Test correct calculation of finished, started, active, canceled metrics of the {@link GridJobProcessor}. */
    @Test
    public void testGridJobMetrics() throws Exception {
        latch = new CountDownLatch(1);

        try (IgniteEx g = startGrid(0)) {
            MetricRegistry mreg = g.context().metric().registry(JOBS_METRICS);

            LongMetric started = mreg.findMetric(STARTED);
            LongMetric active = mreg.findMetric(ACTIVE);
            LongMetric waiting = mreg.findMetric(WAITING);
            LongMetric canceled = mreg.findMetric(CANCELED);
            LongMetric rejected = mreg.findMetric(REJECTED);
            LongMetric finished = mreg.findMetric(FINISHED);
            LongMetric totalExecutionTime = mreg.findMetric(EXECUTION_TIME);
            LongMetric totalWaitingTime = mreg.findMetric(WAITING_TIME);

            assertNotNull(started);
            assertNotNull(active);
            assertNotNull(waiting);
            assertNotNull(canceled);
            assertNotNull(rejected);
            assertNotNull(finished);
            assertNotNull(totalExecutionTime);
            assertNotNull(totalWaitingTime);

            assertEquals(0, started.value());
            assertEquals(0, active.value());
            assertEquals(0, waiting.value());
            assertEquals(0, canceled.value());
            assertEquals(0, rejected.value());
            assertEquals(0, finished.value());
            assertEquals(0, totalExecutionTime.value());
            assertEquals(0, totalWaitingTime.value());

            SimplestTask task = new SimplestTask();

            g.compute().execute(task, 1);

            // Waiting task to finish.
            boolean res = waitForCondition(() -> active.value() == 0, TIMEOUT);

            assertTrue("Active = " + active.value(), res);

            assertEquals(1, started.value());
            assertEquals(0, waiting.value());
            assertEquals(0, canceled.value());
            assertEquals(0, rejected.value());
            assertEquals(1, finished.value());

            // Task should block until latch is down.
            task.block = true;

            ComputeTaskFuture<?> fut = g.compute().executeAsync(task, 1);

            // Waiting task to start execution.
            res = waitForCondition(() -> active.value() == 1, TIMEOUT);

            assertTrue("Active = " + active.value(), res);

            assertEquals(2, started.value());
            assertEquals(0, waiting.value());
            assertEquals(0, canceled.value());
            assertEquals(0, rejected.value());
            assertEquals(1, finished.value());

            Thread.sleep(100); // Sleeping to make sure totalExecutionTime will become more the zero.

            // After latch is down, task should finish.
            latch.countDown();

            fut.get(TIMEOUT);

            res = waitForCondition(() -> active.value() == 0, TIMEOUT);

            assertTrue("Active = " + active.value(), res);
            assertTrue("Execution time should be greater then zero.", totalExecutionTime.value() > 0);

            assertEquals(2, finished.value());

            latch = new CountDownLatch(1);

            fut = g.compute().executeAsync(task, 1);

            res = waitForCondition(() -> active.value() == 1, TIMEOUT);

            assertTrue("Active = " + active.value(), res);

            assertEquals(3, started.value());
            assertEquals(0, waiting.value());
            assertEquals(0, canceled.value());
            assertEquals(0, rejected.value());
            assertEquals(2, finished.value());

            // First cancel task, then allow it to finish.
            fut.cancel();

            latch.countDown();

            res = waitForCondition(() -> active.value() == 0, TIMEOUT);

            assertTrue("Active = " + active.value(), res);

            assertEquals(3, started.value());
            assertEquals(0, waiting.value());
            assertEquals(1, canceled.value());
            assertEquals(0, rejected.value());

            res = waitForCondition(() -> finished.value() == 3, TIMEOUT);

            assertTrue("Finished = " + finished.value(), res);
        }
    }

    /** */
    private static class SimplestJob implements ComputeJob {
        /** */
        private final boolean block;

        /** */
        public SimplestJob(boolean block) {
            this.block = block;
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteException {
            if (block) {
                try {
                    latch.await();
                }
                catch (InterruptedException e) {
                    throw new IgniteException(e);
                }
            }

            return "1";
        }
    }

    /** */
    private static class SimplestTask extends ComputeTaskAdapter<Object, Object> {
        /** */
        volatile boolean block;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable Object arg) throws IgniteException {
            Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

            for (ClusterNode node : subgrid)
                jobs.put(new SimplestJob(block), node);

            return jobs;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object reduce(List<ComputeJobResult> results) throws IgniteException {
            return "1";
        }
    }

    /** */
    @IgniteSpiMultipleInstancesSupport(true)
    public static class GridTestCollision extends IgniteSpiAdapter implements CollisionSpi {
        /** */
        HashMap<ComputeJob, CollisionJobContext> jobs = new HashMap<>();

        /** {@inheritDoc} */
        @Override public void onCollision(CollisionContext ctx) {
            for (CollisionJobContext jobCtx : ctx.waitingJobs())
                jobs.put(jobCtx.getJob(), jobCtx);

        }

        /** {@inheritDoc} */
        @Override public void spiStart(String igniteInstanceName) throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void setExternalCollisionListener(CollisionExternalListener lsnr) {
            // No-op.
        }
    }
}
