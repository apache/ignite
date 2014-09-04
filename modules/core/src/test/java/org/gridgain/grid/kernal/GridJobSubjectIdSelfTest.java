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
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Test job subject ID propagation.
 */
public class GridJobSubjectIdSelfTest extends GridCommonAbstractTest {
    /** Job subject ID. */
    private static volatile UUID taskSubjId;

    /** Job subject ID. */
    private static volatile UUID jobSubjId;

    /** Event subject ID. */
    private static volatile UUID evtSubjId;

    /** First node. */
    private Grid node1;

    /** Second node. */
    private Grid node2;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        node1 = startGrid(1);
        node2 = startGrid(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        node1 = null;
        node2 = null;
    }

    /**
     * Test job subject ID propagation.
     *
     * @throws Exception If failed.
     */
    public void testJobSubjectId() throws Exception {
        node2.events().localListen(new GridPredicate<GridEvent>() {
            @Override public boolean apply(GridEvent evt) {
                GridJobEvent evt0 = (GridJobEvent)evt;

                assert evtSubjId == null;

                evtSubjId = evt0.taskSubjectId();

                return false;
            }
        }, GridEventType.EVT_JOB_STARTED);

        node1.compute().execute(new Task(node2.localNode().id()), null).get();

        assertEquals(taskSubjId, jobSubjId);
        assertEquals(taskSubjId, evtSubjId);
    }

    /**
     * Task class.
     */
    public static class Task extends GridComputeTaskAdapter<Object, Object> {
        /** Target node ID. */
        private UUID targetNodeId;

        /** Session. */
        @GridTaskSessionResource
        private GridComputeTaskSession ses;

        /**
         * Constructor.
         *
         * @param targetNodeId Target node ID.
         */
        public Task(UUID targetNodeId) {
            this.targetNodeId = targetNodeId;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid,
            @Nullable Object arg) throws GridException {
            taskSubjId = ((GridTaskSessionInternal)ses).subjectId();

            GridNode node = null;

            for (GridNode subgridNode : subgrid) {
                if (F.eq(targetNodeId, subgridNode.id())) {
                    node = subgridNode;

                    break;
                }
            }

            assert node != null;

            return Collections.singletonMap(new Job(), node);
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
            return null;
        }
    }

    /**
     * Job class.
     */
    public static class Job extends GridComputeJobAdapter {
        /** Session. */
        @GridTaskSessionResource
        private GridComputeTaskSession ses;

        /** {@inheritDoc} */
        @Nullable @Override public Object execute() throws GridException {
            jobSubjId = ((GridTaskSessionInternal)ses).subjectId();

            return null;
        }
    }
}
