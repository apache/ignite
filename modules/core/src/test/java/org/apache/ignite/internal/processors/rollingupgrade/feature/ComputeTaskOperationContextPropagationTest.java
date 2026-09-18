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

package org.apache.ignite.internal.processors.rollingupgrade.feature;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.compute.ComputeExecutionRejectedException;
import org.apache.ignite.compute.ComputeJobMasterLeaveAware;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridJobExecuteResponse;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestCommandArgument;
import org.apache.ignite.internal.TestCommandResponse;
import org.apache.ignite.internal.TestCommandTask;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.client.thin.ClientServerError;
import org.apache.ignite.internal.thread.context.OperationContext;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.collision.CollisionContext;
import org.apache.ignite.spi.collision.CollisionExternalListener;
import org.apache.ignite.spi.collision.CollisionJobContext;
import org.apache.ignite.spi.collision.CollisionSpi;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.processors.rollingupgrade.RollingUpgradeProcessor.OP_FEATURES_ATTR;
import static org.apache.ignite.internal.processors.rollingupgrade.message.TestMessage.A;
import static org.apache.ignite.internal.processors.rollingupgrade.message.TestMessage.B;
import static org.apache.ignite.internal.processors.rollingupgrade.message.TestMessage.C;
import static org.apache.ignite.internal.processors.rollingupgrade.message.TestMessage.D;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/** */
public class ComputeTaskOperationContextPropagationTest extends AbstractRollingUpgradeManagementApiTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName, String ver) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName, ver);

        if (getTestIgniteInstanceIndex(igniteInstanceName) == 1)
            cfg.setCollisionSpi(new TestCollisionSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        TestCollisionSpi.firstJobQueuedLatch = null;
        TestCollisionSpi.cancelFirstJob = false;

        MasterLeaveAwareTask.jobStartedLatch = new CountDownLatch(1);
        MasterLeaveAwareTask.jobUnblockedLatch = new CountDownLatch(1);
        MasterLeaveAwareTask.masterNodeLeftPrecessedLatch = new CountDownLatch(1);
        MasterLeaveAwareTask.masterNodeLeftProcessorFeatures = null;
    }

    /** */
    @Test
    public void testJobNodeLeftResponseIsProcessedUnderTheTaskContext() throws Exception {
        startGrid(0, "2.21.0");
        startGrid(1, "2.21.0");

        TestRecordingCommunicationSpi.spi(grid(1)).blockMessages((node, msg) -> msg instanceof GridJobExecuteResponse);

        IgniteInternalFuture<TestCommandResponse> fut = runAsync(() -> executeCommandFromClient(
            0,
            1,
            "2.20.0",
            TestCommandTask.class,
            new TestCommandArgument(A, B)));

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(1));

        spi.waitForBlocked(1, getTestTimeout());

        stopGrid(1);

        assertThrowsAnyCause(log, () -> fut.get(getTestTimeout()), ClusterTopologyException.class, "Node has left grid");
    }

    /** */
    @Test
    public void testRejectionTriggeredByAnotherCommandIsProcessedUnderTheTaskContext() throws Exception {
        startGrid(0, "2.21.0");
        startGrid(1, "2.21.0");

        TestCollisionSpi.firstJobQueuedLatch = new CountDownLatch(1);
        TestCollisionSpi.cancelFirstJob = true;

        IgniteInternalFuture<TestCommandResponse> firstCmdFut = runAsync(() -> executeCommandFromClient(
            0,
            1,
            "2.20.0",
            TestCommandTask.class,
            new TestCommandArgument(A, B)));

        assertTrue(TestCollisionSpi.firstJobQueuedLatch.await(getTestTimeout(), MILLISECONDS));

        TestCommandResponse secondCmdRes = executeCommandFromClient(0, 1, "2.21.0", TestCommandTask.class, new TestCommandArgument(A, B));

        assertEquals(createNodeFeatureSet("2.21.0"), secondCmdRes.taskFeatures);

        assertThrowsAnyCause(
            log,
            () -> firstCmdFut.get(getTestTimeout()),
            ComputeExecutionRejectedException.class,
            "Job was cancelled before execution");
    }

    /** */
    @Test
    public void testJobActivatedByAnotherCommandRunsUnderTheJobContext() throws Exception {
        startGrid(0, "2.21.0");
        startGrid(1, "2.21.0");

        TestCollisionSpi.firstJobQueuedLatch = new CountDownLatch(1);

        IgniteInternalFuture<TestCommandResponse> firstCmdFut = runAsync(() -> executeCommandFromClient(
            0,
            1,
            "2.20.0",
            TestCommandTask.class,
            new TestCommandArgument(A, B)));

        assertTrue(TestCollisionSpi.firstJobQueuedLatch.await(getTestTimeout(), MILLISECONDS));

        TestCommandResponse secondCmdRes = executeCommandFromClient(0, 1, "2.21.0", TestCommandTask.class, new TestCommandArgument(A, B));

        assertEquals(createNodeFeatureSet("2.20.0"), firstCmdFut.get(getTestTimeout()).jobFeatures);
        assertEquals(createNodeFeatureSet("2.21.0"), secondCmdRes.jobFeatures);
    }

    /** */
    @Test
    public void testMasterNodeLeftCallbackRunsUnderTheJobContext() throws Exception {
        startGrid(0, "2.21.0");
        startGrid(1, "2.21.0");

        IgniteInternalFuture<TestCommandResponse> fut = runAsync(() -> executeCommandFromClient(
            0,
            1,
            "2.20.0",
            MasterLeaveAwareTask.class,
            new TestCommandArgument(A, B)));

        assertTrue(MasterLeaveAwareTask.jobStartedLatch.await(getTestTimeout(), MILLISECONDS));

        stopGrid(0, true);

        try {
            assertTrue(MasterLeaveAwareTask.masterNodeLeftPrecessedLatch.await(getTestTimeout(), MILLISECONDS));

            assertEquals(createNodeFeatureSet("2.20.0"), MasterLeaveAwareTask.masterNodeLeftProcessorFeatures);
        }
        finally {
            MasterLeaveAwareTask.jobUnblockedLatch.countDown();
        }

        assertThrowsAnyCause(
            log,
            () -> fut.get(getTestTimeout()),
            ClientServerError.class,
            "Task cancelled due to stopping of the grid");
    }

    /** */
    @IgniteSpiMultipleInstancesSupport(true)
    public static class TestCollisionSpi extends IgniteSpiAdapter implements CollisionSpi {
        /** */
        static volatile CountDownLatch firstJobQueuedLatch;

        /** */
        static volatile boolean cancelFirstJob;

        /** {@inheritDoc} */
        @Override public void onCollision(CollisionContext ctx) {
            if (firstJobQueuedLatch == null) {
                ctx.waitingJobs().forEach(CollisionJobContext::activate);

                return;
            }

            List<CollisionJobContext> waitingJobs = new ArrayList<>(ctx.waitingJobs());

            if (waitingJobs.size() == 1) {
                firstJobQueuedLatch.countDown();

                return;
            }

            if (cancelFirstJob)
                waitingJobs.get(0).cancel();
            else
                waitingJobs.get(0).activate();

            waitingJobs.get(1).activate();

            firstJobQueuedLatch = null;
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

    /** Command whose job records the operation context its master-leave callback runs under. */
    public static class MasterLeaveAwareTask extends VisorOneNodeTask<TestCommandArgument, TestCommandResponse> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        static volatile CountDownLatch jobStartedLatch;

        /** */
        static volatile CountDownLatch jobUnblockedLatch;

        /** */
        static volatile CountDownLatch masterNodeLeftPrecessedLatch;

        /** */
        static volatile IgniteNodeFeatureSet masterNodeLeftProcessorFeatures;

        /** {@inheritDoc} */
        @Override protected MasterLeaveAwareJob job(TestCommandArgument arg) {
            return new MasterLeaveAwareJob(arg, debug);
        }
    }

    /** */
    private static class MasterLeaveAwareJob extends VisorJob<TestCommandArgument, TestCommandResponse>
        implements ComputeJobMasterLeaveAware {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        protected MasterLeaveAwareJob(TestCommandArgument arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected TestCommandResponse run(TestCommandArgument arg) {
            MasterLeaveAwareTask.jobStartedLatch.countDown();

            try {
                MasterLeaveAwareTask.jobUnblockedLatch.await();
            }
            catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }

            return new TestCommandResponse(initiatorFeatures(), arg, C, D);
        }

        /** {@inheritDoc} */
        @Override public void onMasterNodeLeft(ComputeTaskSession ses) {
            MasterLeaveAwareTask.masterNodeLeftProcessorFeatures = OperationContext.get(OP_FEATURES_ATTR);
            MasterLeaveAwareTask.masterNodeLeftPrecessedLatch.countDown();
        }
    }
}
