/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.gui.tasks;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.cmd.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Stops nodes.
 */
@GridInternal
public class VisorNodesStopTask extends VisorMultiNodeTask<Void, Void, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Nodes IDs to stop. */
    protected Set<UUID> nodeIds;

    /**
     * {@inheritDoc}
     *
     * @param subgrid
     * @param arg
     */
    @Nullable @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid,
        @Nullable GridBiTuple<Set<UUID>, Void> arg) throws GridException {
        assert arg != null;
        assert arg.get1() != null;

        nodeIds = arg.get1();
        taskArg = arg.get2();

        Map<GridComputeJob, GridNode> map = new HashMap<>();

        // Stop remote nodes or stop local node if it is only node to stop.
        for (GridNode node : subgrid)
            if (nodeIds.contains(node.id()) && (nodeIds.size() == 1 || !node.isLocal()))
                map.put(job(taskArg), node);

        return map;
    }

    /** {@inheritDoc} */
    @Override protected VisorNodesStopJob job(Void arg) {
        return new VisorNodesStopJob(arg);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Void reduce(List<GridComputeJobResult> results) throws GridException {
        // Stop local node after remote.
        if (nodeIds.size() > 1 && nodeIds.contains(g.localNode().id()))
            new VisorNodesStopJob(taskArg).execute();

        return null;
    }

    /**
     * Job that stop node.
     */
    private static class VisorNodesStopJob extends VisorJob<Void, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Formal job argument.
         */
        private VisorNodesStopJob(Void arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override protected Void run(Void arg) throws GridException {
            new Thread(new Runnable() {
                @Override public void run() {
                    GridGain.kill(true);
                }
            }, "grid-stopper").start();

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorNodesStopJob.class, this);
        }
    }
}
