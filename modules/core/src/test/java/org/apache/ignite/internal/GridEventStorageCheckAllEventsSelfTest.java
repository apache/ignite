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

package org.apache.ignite.internal;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskMapAsync;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.compute.ComputeTaskSessionFullSupport;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.compute.ComputeTaskTimeoutException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CheckpointEvent;
import org.apache.ignite.events.DeploymentEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.JobEvent;
import org.apache.ignite.events.TaskEvent;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.resources.TaskSessionResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CHECKPOINT_LOADED;
import static org.apache.ignite.events.EventType.EVT_CHECKPOINT_REMOVED;
import static org.apache.ignite.events.EventType.EVT_CHECKPOINT_SAVED;
import static org.apache.ignite.events.EventType.EVT_JOB_CANCELLED;
import static org.apache.ignite.events.EventType.EVT_JOB_FAILED;
import static org.apache.ignite.events.EventType.EVT_JOB_FINISHED;
import static org.apache.ignite.events.EventType.EVT_JOB_MAPPED;
import static org.apache.ignite.events.EventType.EVT_JOB_QUEUED;
import static org.apache.ignite.events.EventType.EVT_JOB_RESULTED;
import static org.apache.ignite.events.EventType.EVT_JOB_STARTED;
import static org.apache.ignite.events.EventType.EVT_JOB_TIMEDOUT;
import static org.apache.ignite.events.EventType.EVT_TASK_DEPLOYED;
import static org.apache.ignite.events.EventType.EVT_TASK_FAILED;
import static org.apache.ignite.events.EventType.EVT_TASK_FINISHED;
import static org.apache.ignite.events.EventType.EVT_TASK_REDUCED;
import static org.apache.ignite.events.EventType.EVT_TASK_STARTED;
import static org.apache.ignite.events.EventType.EVT_TASK_TIMEDOUT;
import static org.apache.ignite.events.EventType.EVT_TASK_UNDEPLOYED;

/**
 * Test event storage.
 */
@GridCommonTest(group = "Kernal Self")
public class GridEventStorageCheckAllEventsSelfTest extends GridCommonAbstractTest {
    /** */
    private static Ignite ignite;

    /**
     *
     */
    public GridEventStorageCheckAllEventsSelfTest() {
        super(/*start grid*/true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        // TODO: IGNITE-3099 (hotfix the test to check the event order in common case).
        cfg.setPublicThreadPoolSize(1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        ignite = G.ignite(getTestGridName());

        long tstamp = startTimestamp();

        ignite.compute().localDeployTask(GridAllEventsTestTask.class, GridAllEventsTestTask.class.getClassLoader());

        List<Event> evts = pullEvents(tstamp, 1);

        assertEvent(evts.get(0).type(), EVT_TASK_DEPLOYED, evts);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        ignite = null;
    }

    /**
     * @param evtType Actual event type.
     * @param expType Expected event type.
     * @param evts Full list of events.
     */
    private void assertEvent(int evtType, int expType, List<Event> evts) {
        assert evtType == expType : "Invalid event [evtType=" + evtType + ", expectedType=" + expType +
            ", evts=" + evts + ']';
    }

    /**
     * @throws Exception If test failed.
     */
    public void testCheckpointEvents() throws Exception {
        long tstamp = startTimestamp();

        generateEvents(null, new GridAllCheckpointEventsTestJob()).get();

        List<Event> evts = pullEvents(tstamp, 11);

        assertEvent(evts.get(0).type(), EVT_TASK_STARTED, evts);
        assertEvent(evts.get(1).type(), EVT_JOB_MAPPED, evts);
        assertEvent(evts.get(2).type(), EVT_JOB_QUEUED, evts);
        assertEvent(evts.get(3).type(), EVT_JOB_STARTED, evts);
        assertEvent(evts.get(4).type(), EVT_CHECKPOINT_SAVED, evts);
        assertEvent(evts.get(5).type(), EVT_CHECKPOINT_LOADED, evts);
        assertEvent(evts.get(6).type(), EVT_CHECKPOINT_REMOVED, evts);
        assertEvent(evts.get(7).type(), EVT_JOB_RESULTED, evts);
        assertEvent(evts.get(8).type(), EVT_TASK_REDUCED, evts);
        assertEvent(evts.get(9).type(), EVT_TASK_FINISHED, evts);
        assertEvent(evts.get(10).type(), EVT_JOB_FINISHED, evts);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testTaskUndeployEvents() throws Exception {
        final long tstamp = startTimestamp();

        generateEvents(null, new GridAllEventsSuccessTestJob()).get();

        // TODO: IGNITE-3099 (hotfix the test to check the event order in common case).
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                try {
                    List<Event> evts = pullEvents(tstamp, 10);
                    return evts.get(evts.size() - 1).type() == EVT_JOB_FINISHED;
                }
                catch (Exception ignored) {
                    return false;
                }
            }
        }, 500);

        ignite.compute().undeployTask(GridAllEventsTestTask.class.getName());
        ignite.compute().localDeployTask(GridAllEventsTestTask.class, GridAllEventsTestTask.class.getClassLoader());

        List<Event> evts = pullEvents(tstamp, 12);

        assertEvent(evts.get(0).type(), EVT_TASK_STARTED, evts);
        assertEvent(evts.get(1).type(), EVT_JOB_MAPPED, evts);
        assertEvent(evts.get(2).type(), EVT_JOB_QUEUED, evts);
        assertEvent(evts.get(3).type(), EVT_JOB_STARTED, evts);
        assertEvent(evts.get(4).type(), EVT_CHECKPOINT_SAVED, evts);
        assertEvent(evts.get(5).type(), EVT_CHECKPOINT_REMOVED, evts);
        assertEvent(evts.get(6).type(), EVT_JOB_RESULTED, evts);
        assertEvent(evts.get(7).type(), EVT_TASK_REDUCED, evts);
        assertEvent(evts.get(8).type(), EVT_TASK_FINISHED, evts);
        assertEvent(evts.get(9).type(), EVT_JOB_FINISHED, evts);
        assertEvent(evts.get(10).type(), EVT_TASK_UNDEPLOYED, evts);
        assertEvent(evts.get(11).type(), EVT_TASK_DEPLOYED, evts);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testSuccessTask() throws Exception {
        generateEvents(null, new GridAllEventsSuccessTestJob()).get();

        long tstamp = startTimestamp();

        generateEvents(null, new GridAllEventsSuccessTestJob()).get();

        List<Event> evts = pullEvents(tstamp, 10);

        assertEvent(evts.get(0).type(), EVT_TASK_STARTED, evts);
        assertEvent(evts.get(1).type(), EVT_JOB_MAPPED, evts);
        assertEvent(evts.get(2).type(), EVT_JOB_QUEUED, evts);
        assertEvent(evts.get(3).type(), EVT_JOB_STARTED, evts);
        assertEvent(evts.get(4).type(), EVT_CHECKPOINT_SAVED, evts);
        assertEvent(evts.get(5).type(), EVT_CHECKPOINT_REMOVED, evts);
        assertEvent(evts.get(6).type(), EVT_JOB_RESULTED, evts);
        assertEvent(evts.get(7).type(), EVT_TASK_REDUCED, evts);
        assertEvent(evts.get(8).type(), EVT_TASK_FINISHED, evts);
        assertEvent(evts.get(9).type(), EVT_JOB_FINISHED, evts);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testFailTask() throws Exception {
        long tstamp = startTimestamp();

        ComputeTaskFuture<?> fut = generateEvents(null, new GridAllEventsFailTestJob());

        try {
            fut.get();

            assert false : "Grid with locally executed job with timeout should throw ComputeTaskTimeoutException.";
        }
        catch (IgniteException e) {
            info("Expected exception caught [taskFuture=" + fut + ", exception=" + e + ']');
        }

        List<Event> evts = pullEvents(tstamp, 7);

        assertEvent(evts.get(0).type(), EVT_TASK_STARTED, evts);
        assertEvent(evts.get(1).type(), EVT_JOB_MAPPED, evts);
        assertEvent(evts.get(2).type(), EVT_JOB_QUEUED, evts);
        assertEvent(evts.get(3).type(), EVT_JOB_STARTED, evts);
        assertEvent(evts.get(4).type(), EVT_JOB_RESULTED, evts);
        assertEvent(evts.get(5).type(), EVT_TASK_FAILED, evts);
        assertEvent(evts.get(6).type(), EVT_JOB_FAILED, evts);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testTimeoutTask() throws Exception {
        long tstamp = startTimestamp();

        ComputeTaskFuture<?> fut = generateEvents(1000L, new GridAllEventsTimeoutTestJob());

        try {
            fut.get();

            assert false : "Task should fail.";
        }
        catch (ComputeTaskTimeoutException e) {
            info("Expected timeout exception caught [taskFuture=" + fut + ", exception=" + e + ']');
        }

        List<Event> evts = pullEvents(tstamp, 6);

        assertEvent(evts.get(0).type(), EVT_TASK_STARTED, evts);
        assertEvent(evts.get(1).type(), EVT_JOB_MAPPED, evts);
        assertEvent(evts.get(2).type(), EVT_JOB_QUEUED, evts);
        assertEvent(evts.get(3).type(), EVT_JOB_STARTED, evts);

        boolean isTaskTimeout = false;
        boolean isTaskFailed = false;

        for (int i = 4; i < evts.size(); i++) {
            int evtType = evts.get(i).type();

            if (evtType == EVT_TASK_TIMEDOUT) {
                assert !isTaskTimeout;
                assert !isTaskFailed;

                isTaskTimeout = true;
            }
            else if (evtType == EVT_TASK_FAILED) {
                assert isTaskTimeout;
                assert !isTaskFailed;

                isTaskFailed = true;
            }
            else {
                assert evtType == EVT_JOB_CANCELLED
                    || evtType == EVT_JOB_TIMEDOUT
                    || evtType == EVT_JOB_FAILED
                    || evtType == EVT_JOB_FINISHED :
                    "Unexpected event: " + evts.get(i);
            }
        }

        assert isTaskTimeout;
        assert isTaskFailed;
    }

    /**
     * Returns timestamp at the method call moment, but sleeps before return,
     * to allow pass {@link IgniteUtils#currentTimeMillis()}.
     *
     * @return Call timestamp.
     * @throws Exception If failed.
     */
    private long startTimestamp() throws Exception {
        final long tstamp = U.currentTimeMillis();

        Thread.sleep(20);

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return U.currentTimeMillis() > tstamp;
            }
        }, 5000);

        assert U.currentTimeMillis() > tstamp;

        return U.currentTimeMillis();
    }

    /**
     * Pull all test task related events since the given moment.
     *
     * @param since Earliest time to pulled events.
     * @param evtCnt Expected event count
     * @return List of events.
     * @throws Exception If failed.
     */
    private List<Event> pullEvents(long since, int evtCnt) throws Exception {
        IgnitePredicate<Event> filter = new CustomEventFilter(GridAllEventsTestTask.class.getName(), since);

        for (int i = 0; i < 3; i++) {
            List<Event> evts = new ArrayList<>(ignite.events().localQuery((filter)));

            info("Filtered events [size=" + evts.size() + ", evts=" + evts + ']');

            if (evtCnt != evts.size() && i < 2) {
                U.warn(log, "Invalid event count (will retry in 1000 ms) [actual=" + evts.size() +
                    ", expected=" + evtCnt + ", evts=" + evts + ']');

                U.sleep(1000);

                continue;
            }

            assert evtCnt <= evts.size() : "Invalid event count [actual=" + evts.size() + ", expected=" + evtCnt +
                ", evts=" + evts + ']';

            return evts;
        }

        assert false;

        return null;
    }

    /**
     * @param timeout Timeout.
     * @param job Job.
     * @return Task future.
     * @throws Exception If failed.
     */
    private ComputeTaskFuture<?> generateEvents(@Nullable Long timeout, ComputeJob job) throws Exception {
        IgniteCompute comp = ignite.compute().withAsync();

        if (timeout == null)
            comp.execute(GridAllEventsTestTask.class.getName(), job);
        else
            comp.withTimeout(timeout).execute(GridAllEventsTestTask.class.getName(), job);

        return comp.future();
    }

    /**
     *
     */
    private static class CustomEventFilter implements IgnitePredicate<Event> {
        /** */
        private final String taskName;

        /** */
        private final long tstamp;

        /**
         * @param taskName Task name.
         * @param tstamp Timestamp.
         */
        CustomEventFilter(String taskName, long tstamp) {
            assert taskName != null;
            assert tstamp > 0;

            this.taskName = taskName;
            this.tstamp = tstamp;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(Event evt) {
            if (evt.timestamp() >= tstamp) {
                if (evt instanceof TaskEvent)
                    return taskName.equals(((TaskEvent)evt).taskName());
                else if (evt instanceof JobEvent)
                    return taskName.equals(((JobEvent)evt).taskName());
                else if (evt instanceof DeploymentEvent)
                    return taskName.equals(((DeploymentEvent)evt).alias());
                else if (evt instanceof CheckpointEvent)
                    return true;
            }

            return false;
        }
    }

    /**
     *
     */
    private static class GridAllEventsSuccessTestJob extends ComputeJobAdapter {
        /** */
        @TaskSessionResource
        private ComputeTaskSession taskSes;

        /** {@inheritDoc} */
        @Override public String execute() {
            assert taskSes != null;

            taskSes.saveCheckpoint("testCheckpoint", "TestState");
            taskSes.removeCheckpoint("testCheckpoint");

            return "GridAllEventsSuccessTestJob-test-event-success.";
        }
    }

    /**
     *
     */
    private static class GridAllEventsFailTestJob extends ComputeJobAdapter {
        /** {@inheritDoc} */
        @Override public String execute() {
            throw new RuntimeException("GridAllEventsFailTestJob expected test exception.");
        }
    }

    /**
     */
    private static class GridAllEventsTimeoutTestJob extends ComputeJobAdapter {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @SuppressWarnings("BusyWait")
        @Override public String execute() {
            try {
                while (!isCancelled())
                    Thread.sleep(5000);
            }
            catch (InterruptedException ignored) {
                if (log.isInfoEnabled())
                    log.info("GridAllEventsTimeoutTestJob was interrupted.");

                return "GridAllEventsTimeoutTestJob-test-event-timeout.";
            }

            return "GridAllEventsTimeoutTestJob-test-event-timeout.";
        }
    }

    /**
     *
     */
    private static class GridAllCheckpointEventsTestJob extends ComputeJobAdapter {
        /** */
        @TaskSessionResource
        private ComputeTaskSession taskSes;

        /** {@inheritDoc} */
        @Override public String execute() {
            assert taskSes != null;

            taskSes.saveCheckpoint("testAllCheckpoint", "CheckpointTestState");
            taskSes.loadCheckpoint("testAllCheckpoint");
            taskSes.removeCheckpoint("testAllCheckpoint");

            return "GridAllCheckpointEventsSuccess-test-all-checkpoint-event-success.";
        }
    }

    /**
     *
     */
    @ComputeTaskSessionFullSupport
    @ComputeTaskMapAsync // TODO: IGNITE-3099 (hotfix the test to check the event order in common case).
    private static class GridAllEventsTestTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) {
            return Collections.singleton((ComputeJob)arg);
        }

        /** {@inheritDoc} */
        @Override public Serializable reduce(List<ComputeJobResult> results) {
            assert results != null;
            assert results.size() == 1;

            return (Serializable)results;
        }
    }
}
