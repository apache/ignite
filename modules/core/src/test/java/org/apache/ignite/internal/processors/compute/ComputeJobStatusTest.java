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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.collision.GridCollisionManager;
import org.apache.ignite.internal.processors.job.ComputeJobStatusEnum;
import org.apache.ignite.internal.processors.job.GridJobProcessor;
import org.apache.ignite.internal.processors.task.GridTaskProcessor;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.collision.CollisionContext;
import org.apache.ignite.spi.collision.CollisionExternalListener;
import org.apache.ignite.spi.collision.CollisionJobContext;
import org.apache.ignite.spi.collision.priorityqueue.PriorityQueueCollisionSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.Collections.emptyMap;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.job.ComputeJobStatusEnum.CANCELLED;
import static org.apache.ignite.internal.processors.job.ComputeJobStatusEnum.FAILED;
import static org.apache.ignite.internal.processors.job.ComputeJobStatusEnum.FINISHED;
import static org.apache.ignite.internal.processors.job.ComputeJobStatusEnum.QUEUED;
import static org.apache.ignite.internal.processors.job.ComputeJobStatusEnum.RUNNING;
import static org.apache.ignite.internal.processors.job.ComputeJobStatusEnum.SUSPENDED;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;

/**
 * Class for testing {@link GridTaskProcessor#jobStatuses} and {@link GridJobProcessor#jobStatuses}.
 */
public class ComputeJobStatusTest extends GridCommonAbstractTest {
    /** Coordinator. */
    private static IgniteEx node0;

    /** Second node. */
    private static IgniteEx node1;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        stopAllGrids();

        IgniteEx crd = startGrids(2);

        crd.cluster().state(ACTIVE);

        awaitPartitionMapExchange();

        node0 = crd;
        node1 = grid(1);

        // We are changing it because compute jobs fall asleep.
        assertTrue(computeJobWorkerInterruptTimeout(node0).propagate(10L));
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();

        node0 = null;
        node1 = null;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        applyAllNodes(PriorityQueueCollisionSpiEx::reset);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setFailureHandler(new StopNodeFailureHandler())
            .setCollisionSpi(new PriorityQueueCollisionSpiEx())
            // Disable automatic update of metrics, which can call PriorityQueueCollisionSpi.onCollision
            // - leads to the activation (execute) of jobs.
            .setMetricsUpdateFrequency(Long.MAX_VALUE)
            .setClientFailureDetectionTimeout(Long.MAX_VALUE);
    }

    /**
     * Check that there will be no errors if they request statuses for non-existing tasks.
     */
    @Test
    public void testNoStatistics() {
        IgniteUuid sesId = IgniteUuid.fromUuid(UUID.randomUUID());

        checkTaskJobStatuses(sesId, null, null);
        checkJobJobStatuses(sesId, null, null);
    }

    /**
     * Checks that the statuses of the job will be:
     * {@link ComputeJobStatusEnum#QUEUED} -> {@link ComputeJobStatusEnum#RUNNING} -> {@link ComputeJobStatusEnum#FINISHED}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testFinishedTasks() throws Exception {
        checkJobStatuses(FINISHED);
    }

    /**
     * Checks that the statuses of the work will be:
     * {@link ComputeJobStatusEnum#QUEUED} -> {@link ComputeJobStatusEnum#RUNNING} -> {@link ComputeJobStatusEnum#FAILED}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testFailedTasks() throws Exception {
        checkJobStatuses(FAILED);
    }

    /**
     * Checks that the statuses of the work will be:
     * {@link ComputeJobStatusEnum#QUEUED} -> {@link ComputeJobStatusEnum#RUNNING} -> {@link ComputeJobStatusEnum#CANCELLED}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void tesCancelledTasks() throws Exception {
        checkJobStatuses(CANCELLED);
    }

    /**
     * Checks that the statuses of the work will be:
     * {@link ComputeJobStatusEnum#QUEUED} -> {@link ComputeJobStatusEnum#RUNNING} ->
     * {@link ComputeJobStatusEnum#SUSPENDED} -> {@link ComputeJobStatusEnum#FINISHED}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void tesSuspendedTasks() throws Exception {
        checkJobStatuses(SUSPENDED);
    }

    /** */
    private void checkJobStatuses(ComputeJobStatusEnum exp) throws Exception {
        applyAllNodes(spiEx -> spiEx.waitJobCls = WaitJob.class);

        ComputeTaskFuture<Void> taskFut = node0.compute().executeAsync(
            new SimpleTask(() -> new WaitJob(getTestTimeout())),
            null
        );

        // We are waiting for the jobs (PriorityQueueCollisionSpiEx#waitJobCls == WaitJob.class)
        // to be received on the nodes to ensure that the correct statistics are obtained.
        applyAllNodes(spiEx -> spiEx.waitJobFut.get(getTestTimeout()));

        IgniteUuid sesId = taskFut.getTaskSession().getId();

        checkTaskJobStatuses(sesId, QUEUED, null);
        checkJobJobStatuses(sesId, QUEUED, QUEUED);

        PriorityQueueCollisionSpiEx spiEx0 = PriorityQueueCollisionSpiEx.spiEx(node0);
        PriorityQueueCollisionSpiEx spiEx1 = PriorityQueueCollisionSpiEx.spiEx(node1);

        WaitJob waitJob0 = spiEx0.task();
        WaitJob waitJob1 = spiEx1.task();

        // Activating and waiting for a job (WaitJob) to start on node0.
        spiEx0.handleCollisions();
        waitJob0.onStartFut.get(getTestTimeout());

        checkTaskJobStatuses(sesId, RUNNING, null);
        checkJobJobStatuses(sesId, RUNNING, QUEUED);

        // Activating and waiting for a job (WaitJob) to start on node1.
        spiEx1.handleCollisions();
        waitJob1.onStartFut.get(getTestTimeout());

        checkTaskJobStatuses(sesId, RUNNING, null);
        checkJobJobStatuses(sesId, RUNNING, RUNNING);

        switch (exp) {
            case FINISHED:
                // Just letting job (WaitJob) finish on node0.
                waitJob0.waitFut.onDone();
                break;

            case FAILED:
                // Finish the job (WaitJob) with an error on node0.
                waitJob0.waitFut.onDone(new Exception("from test"));
                break;

            case CANCELLED:
                // Cancel the job (WaitJob) on node0.
                node0.context().job().cancelJob(
                    sesId,
                    spiEx0.waitJobFut.result().getJobContext().getJobId(),
                    false
                );
                break;

            case SUSPENDED:
                // Hold the job (WaitJob) with on node0.
                spiEx0.waitJobFut.result().getJobContext().holdcc();
                break;

            default:
                fail("Unknown: " + exp);
        }

        // Let's wait a bit for the operation (above) to complete.
        U.sleep(100);

        checkTaskJobStatuses(sesId, exp, null);

        if (exp == SUSPENDED) {
            // Must resume (unhold) the job (WaitJob) to finish correctly.
            checkJobJobStatuses(sesId, exp, RUNNING);
            waitJob0.waitFut.onDone();
            spiEx0.waitJobFut.result().getJobContext().callcc();

            U.sleep(100);

            checkTaskJobStatuses(sesId, FINISHED, null);

            // Let's wait a bit for the callcc (above) to complete.
            U.sleep(100);
        }

        // Let's check that the job (WaitJob) on the node0 has finished
        // and that the statistics about it will be empty (on node0).
        checkJobJobStatuses(sesId, null, RUNNING);

        // Let's finish the job (WaitJob) on node1.
        waitJob1.waitFut.onDone();

        taskFut.get(getTestTimeout());

        // After the completion of the task, we will no longer receive statistics about it.
        checkTaskJobStatuses(sesId, null, null);
        checkJobJobStatuses(sesId, null, null);
    }

    /** */
    private void checkTaskJobStatuses(
        IgniteUuid sesId,
        @Nullable ComputeJobStatusEnum expN0,
        @Nullable ComputeJobStatusEnum expN1
    ) {
        Map<ComputeJobStatusEnum, Long> exp0 = expN0 == null ? emptyMap() : F.asMap(expN0, 1L);
        Map<ComputeJobStatusEnum, Long> exp1 = expN1 == null ? emptyMap() : F.asMap(expN1, 1L);

        assertEqualsMaps(exp0, node0.context().task().jobStatuses(sesId));
        assertEqualsMaps(exp1, node1.context().task().jobStatuses(sesId));
    }

    /** */
    private void checkJobJobStatuses(
        IgniteUuid sesId,
        @Nullable ComputeJobStatusEnum expN0,
        @Nullable ComputeJobStatusEnum expN1
    ) {
        Map<ComputeJobStatusEnum, Long> exp0 = expN0 == null ? emptyMap() : F.asMap(expN0, 1L);
        Map<ComputeJobStatusEnum, Long> exp1 = expN1 == null ? emptyMap() : F.asMap(expN1, 1L);

        assertEqualsMaps(exp0, node0.context().job().jobStatuses(sesId));
        assertEqualsMaps(exp1, node1.context().job().jobStatuses(sesId));
    }

    /** */
    private void applyAllNodes(ConsumerX<PriorityQueueCollisionSpiEx> c) throws Exception {
        for (Ignite n : G.allGrids())
            c.accept(PriorityQueueCollisionSpiEx.spiEx(n));
    }

    /** */
    private static class PriorityQueueCollisionSpiEx extends PriorityQueueCollisionSpi {
        /** */
        volatile boolean handleCollision;

        /** */
        @Nullable volatile Class<? extends ComputeJob> waitJobCls;

        /** */
        final GridFutureAdapter<CollisionJobContext> waitJobFut = new GridFutureAdapter<>();

        /** {@inheritDoc} */
        @Override public void onCollision(CollisionContext ctx) {
            if (!waitJobFut.isDone()) {
                Class<? extends ComputeJob> waitJobCls = this.waitJobCls;

                if (waitJobCls != null)
                    ctx.waitingJobs().stream()
                        .filter(jobCtx -> waitJobCls.isInstance(jobCtx.getJob()))
                        .findAny()
                        .ifPresent(waitJobFut::onDone);
            }

            if (handleCollision)
                super.onCollision(ctx);
        }

        /** */
        void reset() {
            handleCollision = false;

            waitJobCls = null;

            waitJobFut.reset();
        }

        /** */
        void handleCollisions() {
            handleCollision = true;

            GridCollisionManager collision = ((IgniteEx)ignite).context().collision();

            AtomicReference<CollisionExternalListener> extLsnr = getFieldValue(collision, "extLsnr");

            CollisionExternalListener lsnr = extLsnr.get();

            assertNotNull(lsnr);

            lsnr.onExternalCollision();
        }

        /** */
        <T> T task() {
            return (T)waitJobFut.result().getJob();
        }

        /** */
        static PriorityQueueCollisionSpiEx spiEx(Ignite n) {
            return ((PriorityQueueCollisionSpiEx)n.configuration().getCollisionSpi());
        }
    }

    /** */
    private interface ConsumerX<T> {
        /** */
        void accept(T t) throws Exception;
    }

    /** */
    private static class SimpleTask extends ComputeTaskAdapter<Void, Void> {
        /** */
        final Supplier<? extends ComputeJob> jobFactory;

        /** */
        private SimpleTask(Supplier<? extends ComputeJob> factory) {
            jobFactory = factory;
        }

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(
            List<ClusterNode> subgrid,
            Void arg
        ) throws IgniteException {
            return subgrid.stream().collect(toMap(n -> jobFactory.get(), identity()));
        }

        /** {@inheritDoc} */
        @Override public Void reduce(List<ComputeJobResult> results) throws IgniteException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(
            ComputeJobResult res,
            List<ComputeJobResult> rcvd
        ) throws IgniteException {
            return ComputeJobResultPolicy.WAIT;
        }
    }

    /** */
    private static class WaitJob extends ComputeJobAdapter implements Externalizable {
        /** */
        GridFutureAdapter<Void> onStartFut;

        /** */
        GridFutureAdapter<Void> waitFut;

        /** */
        long waitTimeout;

        /** */
        public WaitJob() {
            onStartFut = new GridFutureAdapter<>();
            waitFut = new GridFutureAdapter<>();
        }

        /** */
        private WaitJob(long timeout) {
            this();

            waitTimeout = timeout;
        }

        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteException {
            onStartFut.onDone();

            try {
                waitFut.get(waitTimeout);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(waitTimeout);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            waitTimeout = in.readLong();
        }
    }
}
