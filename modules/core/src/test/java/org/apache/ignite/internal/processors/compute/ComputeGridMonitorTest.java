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
import java.util.Queue;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.compute.ComputeTaskSessionFullSupport;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.GridTaskSessionImpl;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.task.monitor.ComputeGridMonitor;
import org.apache.ignite.internal.processors.task.monitor.ComputeTaskStatusEnum;
import org.apache.ignite.internal.processors.task.monitor.ComputeTaskStatusSnapshot;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.task.monitor.ComputeTaskStatusEnum.FAILED;
import static org.apache.ignite.internal.processors.task.monitor.ComputeTaskStatusEnum.FINISHED;
import static org.apache.ignite.internal.processors.task.monitor.ComputeTaskStatusEnum.RUNNING;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Test class for {@link ComputeGridMonitor}.
 */
public class ComputeGridMonitorTest extends GridCommonAbstractTest {
    /** Coordinator. */
    private static IgniteEx CRD;

    /** Compute task status monitor. */
    private ComputeGridMonitorImpl monitor;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        stopAllGrids();

        IgniteEx crd = startGrids(2);

        crd.cluster().state(ACTIVE);

        awaitPartitionMapExchange();

        CRD = crd;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();

        CRD = null;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        CRD.context().task().listenStatusUpdates(monitor = new ComputeGridMonitorImpl());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        CRD.context().task().stopListenStatusUpdates(monitor);
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new StopNodeFailureHandler();
    }

    /**
     * Checking get of diffs for the successful execution of the task.
     */
    @Test
    public void simpleTest() {
        ComputeTaskFuture<Void> taskFut = CRD.compute().executeAsync(new NoopComputeTask(), null);

        taskFut.get(getTestTimeout());

        assertTrue(monitor.statusSnapshots.isEmpty());

        assertEquals(3, monitor.statusChanges.size());

        checkTaskStarted(monitor.statusChanges.poll(), taskFut.getTaskSession());
        checkTaskMapped(monitor.statusChanges.poll(), taskFut.getTaskSession());
        checkTaskFinished(monitor.statusChanges.poll(), taskFut.getTaskSession());
    }

    /**
     * Checking get of diffs for the failed execution of the task.
     */
    @Test
    public void failTaskTest() {
        NoopComputeTask task = new NoopComputeTask() {
            /**
             * {@inheritDoc}
             */
            @Override public Void reduce(List<ComputeJobResult> results) throws IgniteException {
                throw new IgniteException("FAIL TASK");
            }
        };

        ComputeTaskFuture<Void> taskFut = CRD.compute().executeAsync(task, null);

        assertThrows(log, () -> taskFut.get(getTestTimeout()), IgniteException.class, null);

        assertTrue(monitor.statusSnapshots.isEmpty());

        assertEquals(3, monitor.statusChanges.size());

        checkTaskStarted(monitor.statusChanges.poll(), taskFut.getTaskSession());
        checkTaskMapped(monitor.statusChanges.poll(), taskFut.getTaskSession());
        checkTaskFailed(monitor.statusChanges.poll(), taskFut.getTaskSession());
    }

    /**
     * Checking get of diffs when changing the task attribute.
     *
     * @throws Exception If failed.
     */
    @Test
    public void changeAttributesTest() throws Exception {
        ComputeFullWithWaitTask task = new ComputeFullWithWaitTask(getTestTimeout());

        ComputeTaskFuture<Void> taskFut = CRD.compute().executeAsync(task, null);

        task.doneOnMapFut.get(getTestTimeout());

        taskFut.getTaskSession().setAttribute("test", "test");

        assertEquals(
            "test",
            taskFut.getTaskSession().waitForAttribute("test", getTestTimeout())
        );

        taskFut.get(getTestTimeout());

        assertTrue(monitor.statusSnapshots.isEmpty());

        assertEquals(4, monitor.statusChanges.size());

        checkTaskStarted(monitor.statusChanges.poll(), taskFut.getTaskSession());
        checkTaskMapped(monitor.statusChanges.poll(), taskFut.getTaskSession());
        checkAttributeChanged(monitor.statusChanges.poll(), taskFut.getTaskSession());
        checkTaskFinished(monitor.statusChanges.poll(), taskFut.getTaskSession());
    }

    /**
     * Checking the get of snapshots of task statuses.
     *
     * @throws Exception If failed.
     */
    @Test
    public void snapshotsTest() throws Exception {
        ComputeFullWithWaitTask task = new ComputeFullWithWaitTask(getTestTimeout());

        ComputeTaskFuture<Void> taskFut = CRD.compute().executeAsync(task, null);

        task.doneOnMapFut.get(getTestTimeout());

        ComputeGridMonitorImpl monitor1 = new ComputeGridMonitorImpl();

        try {
            CRD.context().task().listenStatusUpdates(monitor1);

            assertTrue(monitor.statusSnapshots.isEmpty());

            assertEquals(1, monitor1.statusSnapshots.size());

            checkSnapshot(monitor1.statusSnapshots.poll(), taskFut.getTaskSession());
        }
        finally {
            CRD.context().task().stopListenStatusUpdates(monitor1);
        }

        taskFut.get(getTestTimeout());
    }

    /** */
    private void checkTaskStarted(ComputeTaskStatusSnapshot snapshot, ComputeTaskSession session) {
        checkSnapshot(snapshot, (GridTaskSessionImpl)session, RUNNING, false, false);
    }

    /** */
    private void checkTaskMapped(ComputeTaskStatusSnapshot snapshot, ComputeTaskSession session) {
        checkSnapshot(snapshot, (GridTaskSessionImpl)session, RUNNING, true, false);
    }

    /** */
    private void checkAttributeChanged(ComputeTaskStatusSnapshot snapshot, ComputeTaskSession session) {
        checkSnapshot(snapshot, (GridTaskSessionImpl)session, RUNNING, true, true);
    }

    /** */
    private void checkTaskFinished(ComputeTaskStatusSnapshot snapshot, ComputeTaskSession session) {
        checkSnapshot(snapshot, (GridTaskSessionImpl)session, FINISHED, true, true);
    }

    /** */
    private void checkTaskFailed(ComputeTaskStatusSnapshot snapshot, ComputeTaskSession session) {
        checkSnapshot(snapshot, (GridTaskSessionImpl)session, FAILED, true, true);
    }

    /** */
    private void checkSnapshot(ComputeTaskStatusSnapshot snapshot, ComputeTaskSession session) {
        checkSnapshot(snapshot, (GridTaskSessionImpl)session, RUNNING, true, true);
    }

    /** */
    private void checkSnapshot(
        ComputeTaskStatusSnapshot snapshot,
        GridTaskSessionImpl session,
        ComputeTaskStatusEnum expStatus,
        boolean checkJobNodes,
        boolean checkAttributes
    ) {
        assertEquals(session.getId(), snapshot.sessionId());
        assertEquals(expStatus, snapshot.status());

        assertEquals(session.getTaskName(), snapshot.taskName());
        assertEquals(session.getTaskNodeId(), snapshot.originatingNodeId());
        assertEquals(session.getStartTime(), snapshot.startTime());
        assertEquals(session.isFullSupport(), snapshot.fullSupport());
        assertEquals(session.isInternal(), session.isInternal());

        checkLogin(session, snapshot);

        if (checkJobNodes) {
            assertEquals(
                new TreeSet<>(session.getTopology()),
                new TreeSet<>(snapshot.jobNodes())
            );
        }
        else
            assertTrue(snapshot.jobNodes().isEmpty());

        if (checkAttributes && session.isFullSupport()) {
            assertEquals(
                new TreeMap<>(session.getAttributes()),
                new TreeMap<>(snapshot.attributes())
            );
        }

        if (expStatus == FINISHED) {
            assertTrue(snapshot.endTime() > 0L);
            assertNull(snapshot.failReason());
        }
        else if (expStatus == FAILED) {
            assertTrue(snapshot.endTime() > 0L);
            assertNotNull(snapshot.failReason());
        }
        else {
            assertEquals(0L, snapshot.endTime());
            assertNull(snapshot.failReason());
        }
    }

    /** */
    private static class ComputeGridMonitorImpl implements ComputeGridMonitor {
        /** */
        final Queue<ComputeTaskStatusSnapshot> statusSnapshots = new ConcurrentLinkedQueue<>();

        /** */
        final Queue<ComputeTaskStatusSnapshot> statusChanges = new ConcurrentLinkedQueue<>();

        /** {@inheritDoc} */
        @Override public void processStatusSnapshots(Collection<ComputeTaskStatusSnapshot> snapshots) {
            statusSnapshots.addAll(snapshots);
        }

        /** {@inheritDoc} */
        @Override public void processStatusChange(ComputeTaskStatusSnapshot snapshot) {
            statusChanges.add(snapshot);
        }
    }

    /** */
    private static class NoopComputeTask extends ComputeTaskAdapter<Void, Void> {
        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(
            List<ClusterNode> subgrid,
            Void arg
        ) throws IgniteException {
            return subgrid.stream().collect(toMap(n -> new NoopComputeJob(), identity()));
        }

        /** {@inheritDoc} */
        @Override public Void reduce(List<ComputeJobResult> results) throws IgniteException {
            return null;
        }
    }

    /** */
    @ComputeTaskSessionFullSupport
    private static class ComputeFullWithWaitTask extends ComputeTaskAdapter<Void, Void> {
        /** */
        final GridFutureAdapter<Void> doneOnMapFut = new GridFutureAdapter<>();

        /** */
        final long timeout;

        /** */
        public ComputeFullWithWaitTask(long timeout) {
            this.timeout = timeout;
        }

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(
            List<ClusterNode> subgrid,
            Void arg
        ) throws IgniteException {
            doneOnMapFut.onDone();

            return subgrid.stream().collect(toMap(n -> new NoopComputeJob() {
                /** {@inheritDoc} */
                @Override public Object execute() throws IgniteException {
                    try {
                        U.sleep(500);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        throw new IgniteException(e);
                    }

                    return super.execute();
                }
            }, identity()));
        }

        /** {@inheritDoc} */
        @Override public Void reduce(List<ComputeJobResult> results) throws IgniteException {
            return null;
        }
    }

    /** */
    private static class NoopComputeJob extends ComputeJobAdapter {
        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteException {
            return null;
        }
    }

    /**
     * @param session Task session.
     * @param snapshot Task status snapshot.
     */
    protected void checkLogin(GridTaskSessionImpl session, ComputeTaskStatusSnapshot snapshot) {
        assertNull(session.login());
        assertNull(snapshot.createBy());
    }
}
