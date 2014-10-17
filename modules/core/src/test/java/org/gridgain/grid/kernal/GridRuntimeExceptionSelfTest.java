/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.events.GridEventType.*;

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
        Grid grid = G.grid(getTestGridName());

        grid.compute().localDeployTask(GridTaskFailedTestTask.class, GridTaskFailedTestTask.class.getClassLoader());

        GridComputeTaskFuture<?> fut = grid.compute().execute(GridTaskFailedTestTask.class.getName(), FailType.EXECUTE);

        try {
            fut.get();

            assert false;
        }
        catch (GridException e) {
            info("Got expected grid exception: " + e);
        }

        GridUuid sesId = fut.getTaskSession().getId();

        // Query for correct events.
        List<GridEvent> evts = grid.events().remoteQuery(new TaskFailedEventFilter(sesId), 0).get();

        info("Job failed event: " + evts.get(0));

        assert evts.size() == 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testMapFailed() throws Exception {
        Grid grid = G.grid(getTestGridName());

        grid.compute().localDeployTask(GridTaskFailedTestTask.class, GridTaskFailedTestTask.class.getClassLoader());

        GridComputeTaskFuture<?> fut = grid.compute().execute(GridTaskFailedTestTask.class.getName(), FailType.MAP);

        try {
            fut.get();

            assert false;
        }
        catch (GridException e) {
            info("Got expected grid exception: " + e);
        }

        GridUuid sesId = fut.getTaskSession().getId();

        // Query for correct events.
        List<GridEvent> evts = grid.events().remoteQuery(new TaskFailedEventFilter(sesId), 0).get();

        assert evts.size() == 1;

        info("Task failed event: " + evts.get(0));
    }

    /**
     * @throws Exception If failed.
     */
    public void testResultFailed() throws Exception {
        Grid grid = G.grid(getTestGridName());

        grid.compute().localDeployTask(GridTaskFailedTestTask.class, GridTaskFailedTestTask.class.getClassLoader());

        GridComputeTaskFuture<?> fut = grid.compute().execute(GridTaskFailedTestTask.class.getName(), FailType.RESULT);

        try {
            fut.get();

            assert false;
        }
        catch (GridException e) {
            info("Got expected grid exception: " + e);
        }

        GridUuid sesId = fut.getTaskSession().getId();

        // Query for correct events.
        List<GridEvent> evts = grid.events().remoteQuery(new TaskFailedEventFilter(sesId), 0).get();

        assert evts.size() == 1;

        info("Task failed event: " + evts.get(0));
    }

    /**
     * @throws Exception If failed.
     */
    public void testReduceFailed() throws Exception {
        Grid grid = G.grid(getTestGridName());

        grid.compute().localDeployTask(GridTaskFailedTestTask.class, GridTaskFailedTestTask.class.getClassLoader());

        GridComputeTaskFuture<?> fut = grid.compute().execute(GridTaskFailedTestTask.class.getName(), FailType.RESULT);

        try {
            fut.get();

            assert false;
        }
        catch (GridException e) {
            info("Got expected grid exception: " + e);
        }

        GridUuid sesId = fut.getTaskSession().getId();

        // Query for correct events.
        List<GridEvent> evts = grid.events().remoteQuery(new TaskFailedEventFilter(sesId), 0).get();

        assert evts.size() == 1;

        info("Task failed event: " + evts.get(0));
    }

    /** */
    private static class TaskFailedEventFilter implements GridPredicate<GridEvent> {
        /** */
        private GridUuid sesId;

        /**
         * @param sesId Session ID.
         */
        TaskFailedEventFilter(GridUuid sesId) {
            this.sesId = sesId;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(GridEvent evt) {
            return evt instanceof GridTaskEvent &&
                ((GridTaskEvent)evt).taskSessionId() != null &&
                ((GridTaskEvent)evt).taskSessionId().equals(sesId) &&
                evt.type() == EVT_TASK_FAILED;
        }
    }

    /** */
    private static class GridTaskFailedTestTask extends GridComputeTaskAdapter<Serializable, Serializable> {
        /** */
        @GridLoggerResource private GridLogger log;

        /** */
        @GridLocalNodeIdResource private UUID nodeId;

        /** */
        private FailType failType;

        /** {@inheritDoc} */
        @SuppressWarnings({"ProhibitedExceptionThrown"})
        @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid, Serializable arg)
            throws GridException {
            if (log.isInfoEnabled())
                log.info("Mapping job [job=" + this + ", grid=" + subgrid + ", arg=" + arg + ']');

            failType = (FailType)arg;

            if (failType == FailType.MAP)
                throw new RuntimeException("Failed out of map method.");

            Map<GridComputeJob, GridNode> map = new HashMap<>(2);

            assert subgrid.size() == 1;
            assert subgrid.get(0).id().equals(nodeId);

            map.put(new GridTaskFailedTestJob(null), subgrid.get(0));
            map.put(new GridTaskFailedTestJob(failType), subgrid.get(0));

            return map;
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"ProhibitedExceptionThrown"})
        @Override public GridComputeJobResultPolicy result(GridComputeJobResult res, List<GridComputeJobResult> received) throws GridException {
            if (failType == FailType.RESULT)
                throw new RuntimeException("Failing out of result method.");

            if (res.getException() != null)
                throw res.getException();

            return GridComputeJobResultPolicy.WAIT;
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"ProhibitedExceptionThrown"})
        @Override public Serializable reduce(List<GridComputeJobResult> results) throws GridException {
            assert results != null;

            if (failType == FailType.REDUCE)
                throw new RuntimeException("Failed out of reduce method.");

            return (Serializable)results;
        }
    }

    /** */
    private static class GridTaskFailedTestJob extends GridComputeJobAdapter {
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
