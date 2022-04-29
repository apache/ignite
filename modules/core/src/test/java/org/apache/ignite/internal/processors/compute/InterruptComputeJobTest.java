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

package org.apache.ignite.internal.processors.compute;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobSibling;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.job.GridJobProcessor;
import org.apache.ignite.internal.processors.job.GridJobWorker;
import org.apache.ignite.internal.processors.job.JobWorkerInterruptionTimeoutObject;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.util.GridConcurrentSkipListSet;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.collision.CollisionContext;
import org.apache.ignite.spi.collision.priorityqueue.PriorityQueueCollisionSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.configuration.distributed.DistributedConfigurationProcessor.toMetaStorageKey;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * {@link GridJobWorker} interrupt testing.
 */
public class InterruptComputeJobTest extends GridCommonAbstractTest {
    /** Node. */
    private static IgniteEx node;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        stopAllGrids();

        node = startGrid();

        node.cluster().state(ACTIVE);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();

        node = null;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        PriorityQueueCollisionSpiEx.collisionSpiEx(node).handleCollision = true;

        // Reset distributed property.
        node.context().distributedMetastorage().remove(
            toMetaStorageKey(computeJobWorkerInterruptTimeout(node).getName())
        );
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        // Cleanup with task release.
        CountDownLatchJob.JOBS.removeIf(job -> {
            job.latch.countDown();

            return true;
        });
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setCollisionSpi(new PriorityQueueCollisionSpiEx());
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new StopNodeFailureHandler();
    }

    /**
     * Checks that method {@link GridJobProcessor#computeJobWorkerInterruptTimeout()}
     * returns a valid value that depends on distributed property "computeJobWorkerInterruptTimeout".
     *
     * @throws Exception If failed.
     */
    @Test
    public void testComputeJobWorkerInterruptTimeoutProperty() throws Exception {
        // Check default.
        assertThat(
            node.context().job().computeJobWorkerInterruptTimeout(),
            equalTo(node.context().config().getFailureDetectionTimeout())
        );

        // Check update value.
        computeJobWorkerInterruptTimeout(node).propagate(100500L);

        assertThat(node.context().job().computeJobWorkerInterruptTimeout(), equalTo(100500L));
    }

    /**
     * Checks that when {@link GridJobWorker#cancel()} (even twice) is called, the {@link GridJobWorker#runner()}
     * is not interrupted and that only one {@link JobWorkerInterruptionTimeoutObject} is created.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCancel() throws Exception {
        computeJobWorkerInterruptTimeout(node).propagate(TimeUnit.HOURS.toMillis(1));

        ComputeTaskFuture<Void> taskFut = node.compute().executeAsync(new ComputeTask(CountDownLatchJob.class), null);

        GridJobWorker jobWorker = jobWorker(node, taskFut.getTaskSession());

        cancelWitchChecks(jobWorker);

        cancelWitchChecks(jobWorker);

        ((CountDownLatchJob)jobWorker.getJob()).latch.countDown();

        taskFut.get(getTestTimeout());
    }

    /**
     * Checks that after {@link GridJobWorker#cancel()}, the {@link JobWorkerInterruptionTimeoutObject}
     * will trigger the {@link Thread#interrupt()}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testInterrupt() throws Exception {
        computeJobWorkerInterruptTimeout(node).propagate(100L);

        ComputeTaskFuture<Void> taskFut = node.compute().executeAsync(new ComputeTask(CountDownLatchJob.class), null);

        GridJobWorker jobWorker = jobWorker(node, taskFut.getTaskSession());

        cancelWitchChecks(jobWorker);

        // We are waiting for the GridJobWorkerInterrupter to interrupt the worker.
        taskFut.get(1_000L);

        assertThat(jobWorker.isCancelled(), equalTo(true));
        assertThat(countDownLatchJobInterrupted(jobWorker), equalTo(true));
        assertThat(jobWorkerInterrupters(timeoutObjects(node), jobWorker), empty());
    }

    /**
     * Checks that if the worker was {@link GridJobWorker#cancel()} (even twice) before starting work,
     * then it will be canceled, not interrupted, and have one {@link JobWorkerInterruptionTimeoutObject} before
     * and two after the start.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCancelBeforeStart() throws Exception {
        PriorityQueueCollisionSpiEx.collisionSpiEx(node).handleCollision = false;

        computeJobWorkerInterruptTimeout(node).propagate(TimeUnit.HOURS.toMillis(1));

        ComputeTaskFuture<Void> taskFut = node.compute().executeAsync(new ComputeTask(CountDownLatchJob.class), null);

        GridJobWorker jobWorker = jobWorker(node, taskFut.getTaskSession());

        cancelBeforeStartWitchChecks(jobWorker);

        cancelBeforeStartWitchChecks(jobWorker);

        PriorityQueueCollisionSpiEx.collisionSpiEx(node).handleCollision = true;

        node.context().job().handleCollisions();

        assertTrue(waitForCondition(jobWorker::isStarted, getTestTimeout(), 10));

        assertThat(jobWorker.isCancelled(), equalTo(true));
        assertThat(countDownLatchJobInterrupted(jobWorker), equalTo(false));
        assertThat(jobWorkerInterrupters(timeoutObjects(node), jobWorker), hasSize(2));
    }

    /**
     * @param n Node.
     * @param taskSession Task session.
     * @return Job worker is expected to be the only one and either active or passive.
     */
    private static GridJobWorker jobWorker(IgniteEx n, ComputeTaskSession taskSession) {
        Collection<ComputeJobSibling> siblings = taskSession.getJobSiblings();

        assertThat(siblings, hasSize(1));

        IgniteUuid jobId = F.first(siblings).getJobId();

        GridJobWorker jobWorker = n.context().job().activeJob(jobId);

        if (jobWorker == null) {
            Map<IgniteUuid, GridJobWorker> passiveJobs = getFieldValue(n.context().job(), "passiveJobs");

            if (passiveJobs != null)
                jobWorker = passiveJobs.get(jobId);
        }

        assertThat(jobWorker, notNullValue());

        return jobWorker;
    }

    /**
     * Cancels the worker, checking that it is canceled and not interrupted and only one interrupter is added.
     *
     * @param jobWorker Compute job worker.
     * @throws Exception If failed.
     */
    private void cancelWitchChecks(GridJobWorker jobWorker) throws Exception {
        assertTrue(waitForCondition(jobWorker::isStarted, getTestTimeout(), 10));

        jobWorker.cancel();

        assertThat(jobWorker.isCancelled(), equalTo(true));
        assertThat(countDownLatchJobInterrupted(jobWorker), equalTo(false));
        assertThat(jobWorkerInterrupters(timeoutObjects(node), jobWorker), hasSize(1));
    }

    /**
     * Cancels the worker before it starts, checks that it is canceled, and creates one interrupter.
     *
     * @param jobWorker Compute job worker.
     */
    private void cancelBeforeStartWitchChecks(GridJobWorker jobWorker) {
        jobWorker.cancel();

        assertThat(jobWorker.isStarted(), equalTo(false));
        assertThat(jobWorker.runner(), nullValue());

        assertThat(jobWorker.isCancelled(), equalTo(true));
        assertThat(jobWorkerInterrupters(timeoutObjects(node), jobWorker), hasSize(1));
    }

    /**
     * @param n Node.
     * @return Value of {@code GridTimeoutProcessor#timeoutObjs}.
     */
    private static GridConcurrentSkipListSet<GridTimeoutObject> timeoutObjects(IgniteEx n) {
        return getFieldValue(n.context().timeout(), "timeoutObjs");
    }

    /**
     * @param timeoutObjects Value of {@code GridTimeoutProcessor#timeoutObjs}.
     * @param jobWorker Compute job worker.
     * @return Collection of {@link JobWorkerInterruptionTimeoutObject} for {@code jobWorker}.
     */
    private static Collection<JobWorkerInterruptionTimeoutObject> jobWorkerInterrupters(
        GridConcurrentSkipListSet<GridTimeoutObject> timeoutObjects,
        GridJobWorker jobWorker
    ) {
        return timeoutObjects.stream()
            .filter(JobWorkerInterruptionTimeoutObject.class::isInstance)
            .map(JobWorkerInterruptionTimeoutObject.class::cast)
            .filter(o -> o.jobWorker() == jobWorker)
            .collect(toList());
    }

    /**
     * @return Value of {@link CountDownLatchJob#interrupted}.
     */
    private static boolean countDownLatchJobInterrupted(GridJobWorker jobWorker) {
        return ((CountDownLatchJob)jobWorker.getJob()).interrupted;
    }

    /**
     * Test extension {@link PriorityQueueCollisionSpi}.
     */
    private static class PriorityQueueCollisionSpiEx extends PriorityQueueCollisionSpi {
        /** Collision handling flag. */
        volatile boolean handleCollision = true;

        /** {@inheritDoc} */
        @Override public void onCollision(CollisionContext ctx) {
            if (handleCollision)
                super.onCollision(ctx);
        }

        /**
         * @param n Node.
         * @return Test extension {@link PriorityQueueCollisionSpi}.
         */
        static PriorityQueueCollisionSpiEx collisionSpiEx(IgniteEx n) {
            return ((PriorityQueueCollisionSpiEx)n.configuration().getCollisionSpi());
        }
    }

    /**
     * Task that creates jobs.
     */
    private static class ComputeTask extends ComputeTaskAdapter<Object, Void> {
        /** Compute job class. */
        final Class<? extends ComputeJobAdapter> jobClass;

        /**
         * Constructor.
         *
         * @param jobClass Compute job class.
         */
        ComputeTask(Class<? extends ComputeJobAdapter> jobClass) {
            this.jobClass = jobClass;
        }

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(
            List<ClusterNode> subgrid,
            @Nullable Object arg
        ) throws IgniteException {
            return subgrid.stream().collect(toMap(n -> newComputeJobInstance(arg), identity()));
        }

        /** {@inheritDoc} */
        @Override public Void reduce(List<ComputeJobResult> results) throws IgniteException {
            return null;
        }

        /**
         * @param arg Argument.
         * @return New instance of {@link #jobClass}.
         */
        private ComputeJobAdapter newComputeJobInstance(@Nullable Object arg) {
            try {
                if (arg == null)
                    return jobClass.newInstance();

                return jobClass.getDeclaredConstructor(arg.getClass()).newInstance(arg);
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        }
    }

    /**
     * A job that is waiting for the latch counter to decrement in order to complete its work.
     */
    private static class CountDownLatchJob extends ComputeJobAdapter {
        /** All jobs. */
        static final Collection<CountDownLatchJob> JOBS = new ConcurrentLinkedQueue<>();

        /** Latch. */
        final CountDownLatch latch = new CountDownLatch(1);

        /** Interrupted. */
        volatile boolean interrupted;

        /**
         * Constructor.
         */
        public CountDownLatchJob() {
            JOBS.add(this);
        }

        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteException {
            try {
                latch.await();
            }
            catch (InterruptedException e) {
                interrupted = true;

                Thread.currentThread().interrupt();
            }

            return null;
        }
    }
}
