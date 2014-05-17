/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Base class for multi nodes tasks.
 *
 * @param <A> Task argument type.
 * @param <T> Task result type.
 * @param <J> Job result type.
 */
public abstract class VisorMultiNodeTask<A extends VisorMultiNodeArg, T, J> implements GridComputeTask<A, T> {
    /**
     * Create job that will be mapped to node.
     *
     * @param nid Node ID where job will ba mapped.
     * @param arg Job arguments.
     * @return New job instance that will be mapped to node.
     */
    protected abstract VisorJob<A, J> job(UUID nid, A arg);

    @Nullable @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid,
        @Nullable A arg) throws GridException {
        assert arg != null;

        Map<GridComputeJob, GridNode> map = new HashMap<>();

        for (GridNode node : subgrid)
            if (arg.nodeIds().contains(node.id()))
                map.put(job(node.id(), arg), node);

        return map;
    }

    @Override public GridComputeJobResultPolicy result(GridComputeJobResult res,
        List<GridComputeJobResult> rcvd) throws GridException {
        // All Visor tasks should handle exceptions in reduce method.
        return GridComputeJobResultPolicy.WAIT;
    }
}
