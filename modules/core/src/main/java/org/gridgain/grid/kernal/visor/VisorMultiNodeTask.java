/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor;

import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import static org.gridgain.grid.kernal.visor.util.VisorTaskUtils.*;

import java.util.*;

/**
 * Base class for Visor tasks intended to query data from a multiple node.
 *
 * @param <A> Task argument type.
 * @param <R> Task result type.
 */
public abstract class VisorMultiNodeTask<A, R, J> implements ComputeTask<VisorTaskArgument<A>, R> {
    @IgniteInstanceResource
    protected GridEx g;

    /** Debug flag. */
    protected boolean debug;

    /** Task argument. */
    protected A taskArg;

    /** Task start time. */
    protected long start;

    /**
     * @param arg Task arg.
     * @return New job.
     */
    protected abstract VisorJob<A, J> job(A arg);

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, VisorTaskArgument<A> arg)
        throws GridException {
        assert arg != null;

        start = U.currentTimeMillis();

        debug = arg.debug();

        taskArg = arg.argument();

        if (debug)
            logStart(g.log(), getClass(), start);

        return map0(subgrid, arg);
    }

    /**
     * Actual map logic.
     *
     * @param arg Task execution argument.
     * @param subgrid Nodes available for this task execution.
     * @return Map of grid jobs assigned to subgrid node.
     * @throws GridException If mapping could not complete successfully.
     */
    protected Map<? extends ComputeJob, ClusterNode> map0(List<ClusterNode> subgrid, VisorTaskArgument<A> arg)
        throws GridException {
        Collection<UUID> nodeIds = arg.nodes();

        Map<ComputeJob, ClusterNode> map = U.newHashMap(nodeIds.size());

        try {
            for (ClusterNode node : subgrid)
                if (nodeIds.contains(node.id()))
                    map.put(job(taskArg), node);

            return map;
        }
        finally {
            if (debug)
                logMapped(g.log(), getClass(), map.values());
        }
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res,
        List<ComputeJobResult> rcvd) throws GridException {
        // All Visor tasks should handle exceptions in reduce method.
        return ComputeJobResultPolicy.WAIT;
    }

    /**
     * Actual reduce logic.
     *
     * @param results Job results.
     * @return Task result.
     * @throws GridException If reduction or results caused an error.
     */
    @Nullable protected abstract R reduce0(List<ComputeJobResult> results) throws GridException;

    /** {@inheritDoc} */
    @Nullable @Override public final R reduce(List<ComputeJobResult> results) throws GridException {
        try {
            return reduce0(results);
        }
        finally {
            if (debug)
                logFinish(g.log(), getClass(), start);
        }
    }
}
