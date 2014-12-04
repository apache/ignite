/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;

import static org.apache.ignite.events.GridEventType.*;

/**
 * Tests runtime exception.
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
@GridCommonTest(group = "Kernal Self")
public class GridRuntimeExceptionSelfTest extends GridCommonAbstractTest {
    /** */
    private enum FailType {
        /** */
        MAP,

        /** */
        RESULT,

        /** */
        REDUCE,

        /** */
        EXECUTE
    }

    /** */
    public GridRuntimeExceptionSelfTest() {
        super(/*start grid*/false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopGrid();
    }

    /**
     * @throws Exception If failed.
     */
    public void testExecuteFailed() throws Exception {
        Ignite ignite = G.grid(getTestGridName());

        ignite.compute().localDeployTask(GridTaskFailedTestTask.class, GridTaskFailedTestTask.class.getClassLoader());

        ComputeTaskFuture<?> fut =
            executeAsync(ignite.compute(), GridTaskFailedTestTask.class.getName(), FailType.EXECUTE);

        try {
            fut.get();

            assert false;
        }
        catch (GridException e) {
            info("Got expected grid exception: " + e);
        }

        IgniteUuid sesId = fut.getTaskSession().getId();

        // Query for correct events.
        List<IgniteEvent> evts = ignite.events().remoteQuery(new TaskFailedEventFilter(sesId), 0);

        info("Job failed event: " + evts.get(0));

        assert evts.size() == 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testMapFailed() throws Exception {
        Ignite ignite = G.grid(getTestGridName());

        ignite.compute().localDeployTask(GridTaskFailedTestTask.class, GridTaskFailedTestTask.class.getClassLoader());

        ComputeTaskFuture<?> fut =
            executeAsync(ignite.compute(), GridTaskFailedTestTask.class.getName(), FailType.MAP);

        try {
            fut.get();

            assert false;
        }
        catch (GridException e) {
            info("Got expected grid exception: " + e);
        }

        IgniteUuid sesId = fut.getTaskSession().getId();

        // Query for correct events.
        List<IgniteEvent> evts = ignite.events().remoteQuery(new TaskFailedEventFilter(sesId), 0);

        assert evts.size() == 1;

        info("Task failed event: " + evts.get(0));
    }

    /**
     * @throws Exception If failed.
     */
    public void testResultFailed() throws Exception {
        Ignite ignite = G.grid(getTestGridName());

        ignite.compute().localDeployTask(GridTaskFailedTestTask.class, GridTaskFailedTestTask.class.getClassLoader());

        ComputeTaskFuture<?> fut =
            executeAsync(ignite.compute(), GridTaskFailedTestTask.class.getName(), FailType.RESULT);

        try {
            fut.get();

            assert false;
        }
        catch (GridException e) {
            info("Got expected grid exception: " + e);
        }

        IgniteUuid sesId = fut.getTaskSession().getId();

        // Query for correct events.
        List<IgniteEvent> evts = ignite.events().remoteQuery(new TaskFailedEventFilter(sesId), 0);

        assert evts.size() == 1;

        info("Task failed event: " + evts.get(0));
    }

    /**
     * @throws Exception If failed.
     */
    public void testReduceFailed() throws Exception {
        Ignite ignite = G.grid(getTestGridName());

        ignite.compute().localDeployTask(GridTaskFailedTestTask.class, GridTaskFailedTestTask.class.getClassLoader());

        ComputeTaskFuture<?> fut =
            executeAsync(ignite.compute(), GridTaskFailedTestTask.class.getName(), FailType.RESULT);

        try {
            fut.get();

            assert false;
        }
        catch (GridException e) {
            info("Got expected grid exception: " + e);
        }

        IgniteUuid sesId = fut.getTaskSession().getId();

        // Query for correct events.
        List<IgniteEvent> evts = ignite.events().remoteQuery(new TaskFailedEventFilter(sesId), 0);

        assert evts.size() == 1;

        info("Task failed event: " + evts.get(0));
    }

    /** */
    private static class TaskFailedEventFilter implements IgnitePredicate<IgniteEvent> {
        /** */
        private IgniteUuid sesId;

        /**
         * @param sesId Session ID.
         */
        TaskFailedEventFilter(IgniteUuid sesId) {
            this.sesId = sesId;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(IgniteEvent evt) {
            return evt instanceof GridTaskEvent &&
                ((GridTaskEvent)evt).taskSessionId() != null &&
                ((GridTaskEvent)evt).taskSessionId().equals(sesId) &&
                evt.type() == EVT_TASK_FAILED;
        }
    }

    /** */
    private static class GridTaskFailedTestTask extends ComputeTaskAdapter<Serializable, Serializable> {
        /** */
        @GridLoggerResource private GridLogger log;

        /** */
        @GridLocalNodeIdResource private UUID nodeId;

        /** */
        private FailType failType;

        /** {@inheritDoc} */
        @SuppressWarnings({"ProhibitedExceptionThrown"})
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Serializable arg)
            throws GridException {
            if (log.isInfoEnabled())
                log.info("Mapping job [job=" + this + ", grid=" + subgrid + ", arg=" + arg + ']');

            failType = (FailType)arg;

            if (failType == FailType.MAP)
                throw new RuntimeException("Failed out of map method.");

            Map<ComputeJob, ClusterNode> map = new HashMap<>(2);

            assert subgrid.size() == 1;
            assert subgrid.get(0).id().equals(nodeId);

            map.put(new GridTaskFailedTestJob(null), subgrid.get(0));
            map.put(new GridTaskFailedTestJob(failType), subgrid.get(0));

            return map;
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"ProhibitedExceptionThrown"})
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> received) throws GridException {
            if (failType == FailType.RESULT)
                throw new RuntimeException("Failing out of result method.");

            if (res.getException() != null)
                throw res.getException();

            return ComputeJobResultPolicy.WAIT;
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"ProhibitedExceptionThrown"})
        @Override public Serializable reduce(List<ComputeJobResult> results) throws GridException {
            assert results != null;

            if (failType == FailType.REDUCE)
                throw new RuntimeException("Failed out of reduce method.");

            return (Serializable)results;
        }
    }

    /** */
    private static class GridTaskFailedTestJob extends ComputeJobAdapter {
        /** */
        @GridLoggerResource private GridLogger log;

        /** */
        GridTaskFailedTestJob() {
            // No-op.
        }

        /**
         * @param arg Job argument.
         */
        GridTaskFailedTestJob(FailType arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override public Serializable execute() {
            if (log.isInfoEnabled())
                log.info("Executing job [job=" + this + ", arg=" + argument(0) + ']');

            if (argument(0) != null && argument(0) == FailType.EXECUTE) {
                // Throw exception.
                throw new RuntimeException("GridTaskFailedTestJob expected exception.");
            }

            return true;
        }
    }
}
