/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

import static org.apache.ignite.events.IgniteEventType.*;

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
    @Override protected void beforeTestsStarted() throws Exception {
        ignite = G.grid(getTestGridName());

        long tstamp = startTimestamp();

        ignite.compute().localDeployTask(GridAllEventsTestTask.class, GridAllEventsTestTask.class.getClassLoader());

        List<IgniteEvent> evts = pullEvents(tstamp, 1);

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
    private void assertEvent(int evtType, int expType, List<IgniteEvent> evts) {
        assert evtType == expType : "Invalid event [evtType=" + evtType + ", expectedType=" + expType +
            ", evts=" + evts + ']';
    }

    /**
     * @throws Exception If test failed.
     */
    public void testCheckpointEvents() throws Exception {
        long tstamp = startTimestamp();

        generateEvents(null, new GridAllCheckpointEventsTestJob()).get();

        List<IgniteEvent> evts = pullEvents(tstamp, 11);

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
        long tstamp = startTimestamp();

        generateEvents(null, new GridAllEventsSuccessTestJob()).get();

        ignite.compute().undeployTask(GridAllEventsTestTask.class.getName());
        ignite.compute().localDeployTask(GridAllEventsTestTask.class, GridAllEventsTestTask.class.getClassLoader());

        List<IgniteEvent> evts = pullEvents(tstamp, 12);

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
        long tstamp = startTimestamp();

        generateEvents(null, new GridAllEventsSuccessTestJob()).get();

        List<IgniteEvent> evts = pullEvents(tstamp, 10);

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

            assert false : "Grid with locally executed job with timeout should throw GridComputeTaskTimeoutException.";
        }
        catch (GridException e) {
            info("Expected exception caught [taskFuture=" + fut + ", exception=" + e + ']');
        }

        List<IgniteEvent> evts = pullEvents(tstamp, 7);

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

        List<IgniteEvent> evts = pullEvents(tstamp, 6);

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
     * to allow pass {@link GridUtils#currentTimeMillis()}.
     *
     * @return Call timestamp.
     * @throws InterruptedException If sleep was interrupted.
     */
    private long startTimestamp() throws InterruptedException {
        long tstamp = System.currentTimeMillis();

        Thread.sleep(20);

        return tstamp;
    }

    /**
     * Pull all test task related events since the given moment.
     *
     * @param since Earliest time to pulled events.
     * @param evtCnt Expected event count
     * @return List of events.
     * @throws Exception If failed.
     */
    private List<IgniteEvent> pullEvents(long since, int evtCnt) throws Exception {
        IgnitePredicate<IgniteEvent> filter = new CustomEventFilter(GridAllEventsTestTask.class.getName(), since);

        for (int i = 0; i < 3; i++) {
            List<IgniteEvent> evts = new ArrayList<>(ignite.events().localQuery((filter)));

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
        IgniteCompute comp = ignite.compute().enableAsync();

        if (timeout == null)
            comp.execute(GridAllEventsTestTask.class.getName(), job);
        else
            comp.withTimeout(timeout).execute(GridAllEventsTestTask.class.getName(), job);

        return comp.future();
    }

    /**
     *
     */
    private static class CustomEventFilter implements IgnitePredicate<IgniteEvent> {
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
        @Override public boolean apply(IgniteEvent evt) {
            if (evt.timestamp() >= tstamp) {
                if (evt instanceof IgniteTaskEvent)
                    return taskName.equals(((IgniteTaskEvent)evt).taskName());
                else if (evt instanceof IgniteJobEvent)
                    return taskName.equals(((IgniteJobEvent)evt).taskName());
                else if (evt instanceof IgniteDeploymentEvent)
                    return taskName.equals(((IgniteDeploymentEvent)evt).alias());
                else if (evt instanceof IgniteCheckpointEvent)
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
        @IgniteTaskSessionResource
        private ComputeTaskSession taskSes;

        /** {@inheritDoc} */
        @Override public String execute() throws GridException {
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
        @IgniteLoggerResource
        private GridLogger log;

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
        @IgniteTaskSessionResource
        private ComputeTaskSession taskSes;

        /** {@inheritDoc} */
        @Override public String execute() throws GridException {
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
    private static class GridAllEventsTestTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) throws GridException {
            return Collections.singleton((ComputeJob)arg);
        }

        /** {@inheritDoc} */
        @Override public Serializable reduce(List<ComputeJobResult> results) throws GridException {
            assert results != null;
            assert results.size() == 1;

            return (Serializable)results;
        }
    }
}
