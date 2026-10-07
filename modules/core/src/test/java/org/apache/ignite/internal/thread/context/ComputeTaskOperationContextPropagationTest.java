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


package org.apache.ignite.internal.thread.context;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeJobMasterLeaveAware;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridJobExecuteResponse;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.authentication.User;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.resources.JobContextResource;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.collision.CollisionContext;
import org.apache.ignite.spi.collision.CollisionExternalListener;
import org.apache.ignite.spi.collision.CollisionJobContext;
import org.apache.ignite.spi.collision.CollisionSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;
import static org.apache.ignite.internal.thread.context.OperationContextDispatcher.MAX_ATTRS_CNT;

/** */
public class ComputeTaskOperationContextPropagationTest extends GridCommonAbstractTest {
    /** */
    private static final OperationContextAttribute<User> USR_ATTR = OperationContextAttribute.newInstance();

    /** */
    private static final User FIRST_USR_VAL = User.create("0", "0");

    /** */
    private static final User SECOND_USR_VAL = User.create("1", "1");

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());
        cfg.setPluginProviders(new TestIgniteComponent());

        if (getTestIgniteInstanceIndex(igniteInstanceName) == 1)
            cfg.setCollisionSpi(new TestCollisionSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        TestCollisionSpi.firstJobAction = null;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** */
    @Test
    public void testJobNodeLeftResponseIsProcessedUnderTheTaskContext() throws Exception {
        startGrids(2);

        spi(grid(1)).blockMessages((node, msg) -> msg instanceof GridJobExecuteResponse);

        IgniteFuture<TaskExecution> fut = executeWithContext(new RegularJob(), FIRST_USR_VAL);

        spi(grid(1)).waitForBlocked(1, getTestTimeout());

        stopGrid(1);

        assertEquals(new TaskExecution(FIRST_USR_VAL, FIRST_USR_VAL, null), fut.get(getTestTimeout()));
    }

    /** */
    @Test
    public void testRejectionTriggeredByAnotherTaskIsProcessedUnderTheTaskContext() throws Exception {
        startGrids(2);

        TestCollisionSpi.holdFirstJob(FirstJobAction.REJECT_QUEUED);

        IgniteFuture<TaskExecution> firstTaskFut = executeWithContext(new RegularJob(), FIRST_USR_VAL);

        assertTrue(TestCollisionSpi.firstJobArrivedLatch.await(getTestTimeout(), MILLISECONDS));

        TaskExecution secondExecRes = executeWithContext(new RegularJob(), SECOND_USR_VAL).get(getTestTimeout());
        TaskExecution firstExecRes = firstTaskFut.get(getTestTimeout());

        assertEquals(new TaskExecution(FIRST_USR_VAL, FIRST_USR_VAL, null), firstExecRes);
        assertExecutedWith(SECOND_USR_VAL, secondExecRes);
    }

    /** */
    @Test
    public void testJobActivatedByAnotherTaskRunsUnderTheJobContext() throws Exception {
        startGrids(2);

        TestCollisionSpi.holdFirstJob(FirstJobAction.ACTIVATE_QUEUED);

        IgniteFuture<TaskExecution> firstFut = executeWithContext(new RegularJob(), FIRST_USR_VAL);

        assertTrue(TestCollisionSpi.firstJobArrivedLatch.await(getTestTimeout(), MILLISECONDS));

        TaskExecution secondExecRes = executeWithContext(new RegularJob(), SECOND_USR_VAL).get(getTestTimeout());
        TaskExecution firstExecRes = firstFut.get(getTestTimeout());

        assertExecutedWith(FIRST_USR_VAL, firstExecRes);
        assertExecutedWith(SECOND_USR_VAL, secondExecRes);
    }

    /** */
    @Test
    public void testJobCancelledByAnotherTaskIsCancelledUnderTheJobContext() throws Exception {
        startGrids(2);

        TestCollisionSpi.holdFirstJob(FirstJobAction.CANCEL_RUNNING);

        IgniteFuture<TaskExecution> firstFut = executeWithContext(new CancellableJob(), FIRST_USR_VAL);

        assertTrue(TestCollisionSpi.firstJobArrivedLatch.await(getTestTimeout(), MILLISECONDS));

        TaskExecution secondExecRes = executeWithContext(new RegularJob(), SECOND_USR_VAL).get(getTestTimeout());
        TaskExecution firstExecRes = firstFut.get(getTestTimeout());

        assertExecutedWith(FIRST_USR_VAL, firstExecRes);
        assertExecutedWith(SECOND_USR_VAL, secondExecRes);
    }

    /** */
    @Test
    public void testSuspendedJobResumedUnderAnotherContextRunsUnderTheJobContext() throws Exception {
        startGrids(2);

        SuspendedJob.jobSuspendedLatch = new CountDownLatch(1);
        SuspendedJob.resumeFut = new CompletableFuture<>();

        IgniteFuture<TaskExecution> fut = executeWithContext(new SuspendedJob(), FIRST_USR_VAL);

        assertTrue(SuspendedJob.jobSuspendedLatch.await(getTestTimeout(), MILLISECONDS));

        try (Scope ignored = OperationContext.set(USR_ATTR, SECOND_USR_VAL)) {
            SuspendedJob.resumeFut.complete(null);
        }

        assertExecutedWith(FIRST_USR_VAL, fut.get(getTestTimeout()));
    }

    /** */
    @Test
    public void testMasterNodeLeftCallbackRunsUnderTheJobContext() throws Exception {
        startGrids(2);

        MasterLeaveAwareJob.jobStartedLatch = new CountDownLatch(1);
        MasterLeaveAwareJob.jobUnblockedLatch = new CountDownLatch(1);
        MasterLeaveAwareJob.masterNodeLeftProcessedLatch = new CountDownLatch(1);
        MasterLeaveAwareJob.masterNodeLeftAttr = null;

        executeWithContext(new MasterLeaveAwareJob(), FIRST_USR_VAL);

        try {
            assertTrue(MasterLeaveAwareJob.jobStartedLatch.await(getTestTimeout(), MILLISECONDS));

            stopGrid(0, true);

            assertTrue(MasterLeaveAwareJob.masterNodeLeftProcessedLatch.await(getTestTimeout(), MILLISECONDS));

            assertEquals(FIRST_USR_VAL, MasterLeaveAwareJob.masterNodeLeftAttr);
        }
        finally {
            MasterLeaveAwareJob.jobUnblockedLatch.countDown();
        }
    }

    /** */
    private static void assertExecutedWith(User expAttrVal, TaskExecution exec) {
        assertEquals(new TaskExecution(expAttrVal, expAttrVal, expAttrVal), exec);
    }

    /** */
    private IgniteFuture<TaskExecution> executeWithContext(ComputeJob job, User attrVal) {
        IgniteEx initiator = grid(0);

        try (Scope ignored = OperationContext.set(USR_ATTR, attrVal)) {
            return initiator.compute(initiator.cluster().forNodeId(grid(1).localNode().id())).executeAsync(SingleJobTask.class, job);
        }
    }

    /** */
    private record TaskExecution(
        @Nullable User attributeSeenByTaskResult,
        @Nullable User attributeSeenByTaskReduce,
        @Nullable User attributeSeenByJob
    ) {}

    /** */
    private static class SingleJobTask extends ComputeTaskAdapter<ComputeJob, TaskExecution> {
        /** */
        private @Nullable User seenByResult;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, ComputeJob job) {
            return Map.of(job, subgrid.get(0));
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
            seenByResult = OperationContext.get(USR_ATTR);

            return ComputeJobResultPolicy.REDUCE;
        }

        /** {@inheritDoc} */
        @Override public TaskExecution reduce(List<ComputeJobResult> results) {
            User seenByReduce = OperationContext.get(USR_ATTR);

            return new TaskExecution(seenByResult, seenByReduce, results.get(0).getData());
        }
    }

    /** */
    private static class RegularJob extends ComputeJobAdapter {
        /** {@inheritDoc} */
        @Override public Object execute() {
            return OperationContext.get(USR_ATTR);
        }
    }

    /** */
    private static class CancellableJob extends ComputeJobAdapter {
        /** */
        private final CountDownLatch jobCancelledLatch = new CountDownLatch(1);

        /** */
        private @Nullable User cancelAttr;

        /** {@inheritDoc} */
        @Override public Object execute() {
            try {
                jobCancelledLatch.await();
            }
            catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }

            return cancelAttr;
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            cancelAttr = OperationContext.get(USR_ATTR);

            jobCancelledLatch.countDown();
        }
    }

    /** */
    private static class SuspendedJob extends ComputeJobAdapter {
        /** */
        static volatile CompletableFuture<Void> resumeFut;

        /** */
        static volatile CountDownLatch jobSuspendedLatch;

        /** */
        @JobContextResource
        private transient ComputeJobContext ctx;

        /** */
        private boolean suspended;

        /** {@inheritDoc} */
        @Override public Object execute() {
            if (suspended)
                return OperationContext.get(USR_ATTR);

            suspended = true;

            resumeFut.thenRun(ctx::callcc);

            ctx.holdcc();

            jobSuspendedLatch.countDown();

            return null;
        }
    }

    /** */
    private static class MasterLeaveAwareJob extends ComputeJobAdapter implements ComputeJobMasterLeaveAware {
        /** */
        static volatile CountDownLatch jobStartedLatch;

        /** */
        static volatile CountDownLatch jobUnblockedLatch;

        /** */
        static volatile CountDownLatch masterNodeLeftProcessedLatch;

        /** */
        static volatile User masterNodeLeftAttr;

        /** {@inheritDoc} */
        @Override public Object execute() {
            jobStartedLatch.countDown();

            try {
                jobUnblockedLatch.await();
            }
            catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override public void onMasterNodeLeft(ComputeTaskSession ses) {
            masterNodeLeftAttr = OperationContext.get(USR_ATTR);

            masterNodeLeftProcessedLatch.countDown();
        }
    }

    /** */
    @IgniteSpiMultipleInstancesSupport(true)
    public static class TestCollisionSpi extends IgniteSpiAdapter implements CollisionSpi {
        /** */
        static volatile @Nullable FirstJobAction firstJobAction;

        /** */
        static volatile CountDownLatch firstJobArrivedLatch;

        /** */
        static void holdFirstJob(FirstJobAction action) {
            firstJobArrivedLatch = new CountDownLatch(1);
            firstJobAction = action;
        }

        /** {@inheritDoc} */
        @Override public void onCollision(CollisionContext ctx) {
            FirstJobAction action = firstJobAction;

            List<CollisionJobContext> waitingJobs = new ArrayList<>(ctx.waitingJobs());

            if (action == null) {
                waitingJobs.forEach(CollisionJobContext::activate);

                return;
            }

            if (waitingJobs.isEmpty())
                return;

            List<CollisionJobContext> activeJobs = new ArrayList<>(ctx.activeJobs());

            if (activeJobs.isEmpty() && waitingJobs.size() == 1) {
                if (action == FirstJobAction.CANCEL_RUNNING)
                    waitingJobs.get(0).activate();

                firstJobArrivedLatch.countDown();

                return;
            }

            CollisionJobContext firstJob = action == FirstJobAction.CANCEL_RUNNING ? activeJobs.get(0) : waitingJobs.get(0);
            CollisionJobContext secondJob = waitingJobs.get(waitingJobs.size() - 1);

            switch (action) {
                case ACTIVATE_QUEUED:
                    firstJob.activate();

                    break;

                case REJECT_QUEUED:
                case CANCEL_RUNNING:
                    firstJob.cancel();

                    break;
            }

            secondJob.activate();

            firstJobAction = null;
        }

        /** {@inheritDoc} */
        @Override public void setExternalCollisionListener(CollisionExternalListener lsnr) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStart(String igniteInstanceName) throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws IgniteSpiException {
            // No-op.
        }
    }

    /** */
    private enum FirstJobAction {
        /** */
        ACTIVATE_QUEUED,

        /** */
        REJECT_QUEUED,

        /** */
        CANCEL_RUNNING
    }

    /** */
    private static class TestIgniteComponent extends AbstractTestPluginProvider {
        /** {@inheritDoc} */
        @Override public String name() {
            return "TestComputeTaskOperationContextAttributeRegistrator";
        }

        /** {@inheritDoc} */
        @Override public void start(PluginContext ctx) {
            ((IgniteEx)ctx.grid()).context().operationContextDispatcher().registerDistributedAttribute(MAX_ATTRS_CNT - 1, USR_ATTR);
        }
    }
}
