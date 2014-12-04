/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

import static org.gridgain.grid.kernal.GridNodeAttributes.*;

/**
 * Special kill task that never fails over jobs.
 */
@GridInternal
class GridKillTask extends GridComputeTaskAdapter<Boolean, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Restart flag. */
    private boolean restart;

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Boolean restart)
        throws GridException {
        assert restart != null;

        this.restart = restart;

        Map<ComputeJob, ClusterNode> jobs = U.newHashMap(subgrid.size());

        for (ClusterNode n : subgrid)
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
    private boolean daemon(ClusterNode n) {
        return "true".equalsIgnoreCase(n.<String>attribute(ATTR_DAEMON));
    }

    /** {@inheritDoc} */
    @Override public GridComputeJobResultPolicy result(GridComputeJobResult res, List<GridComputeJobResult> rcvd) {
        return GridComputeJobResultPolicy.WAIT;
    }

    /** {@inheritDoc} */
    @Override public Void reduce(List<GridComputeJobResult> results) throws GridException {
        return null;
    }

    /**
     * Kill job.
     */
    private class GridKillJob extends ComputeJobAdapter {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public Object execute() throws GridException {
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

            return null;
        }
    }
}
