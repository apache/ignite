// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.compute.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.util.typedef.*;

import java.util.*;

import static org.gridgain.grid.kernal.GridNodeAttributes.*;

/**
 * Special kill task that never fails over jobs.
 *
 * @author @java.author
 * @version @java.version
 */
@GridInternal
class GridKillTask extends GridComputeTaskNoReduceAdapter<Boolean> {
    /** Restart flag. */
    private boolean restart;

    /** {@inheritDoc} */
    @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid, Boolean restart)
        throws GridException {
        assert restart != null;

        this.restart = restart;

        Map<GridComputeJob, GridNode> jobs = new HashMap<>(subgrid.size());

        for (GridNode n : subgrid)
            if (!daemon(n))
                jobs.put(new GridKillJob(), n);

        return jobs;
    }

    /**
     * Checks if given node is a daemon node.
     *
     * @param n Node.
     * @return Whether node is daemon.
     */
    private boolean daemon(GridNode n) {
        return "true".equalsIgnoreCase(n.<String>attribute(ATTR_DAEMON));
    }

    /** {@inheritDoc} */
    @Override public GridComputeJobResultPolicy result(GridComputeJobResult res, List<GridComputeJobResult> rcvd) {
        return GridComputeJobResultPolicy.WAIT;
    }

    /**
     * Kill job.
     *
     * @author @java.author
     * @version @java.version
     */
    private class GridKillJob extends GridComputeJobAdapter {
        /** {@inheritDoc} */
        @Override public Object execute() throws GridException {
            return null;
        }

        /**
         * Restarts or kills nodes.
         */
        @GridComputeJobAfterExecute
        public void afterSend() {
            if (restart)
                new Thread(new Runnable() {
                    @Override public void run() {
                        G.restart(true);
                    }
                },
                "grid-restarter").start();
            else
                new Thread(new Runnable() {
                    @Override public void run() {
                        G.kill(true);
                    }
                },
                "grid-stopper").start();
        }
    }
}
